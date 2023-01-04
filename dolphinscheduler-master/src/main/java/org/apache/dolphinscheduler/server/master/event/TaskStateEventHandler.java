/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.master.event;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.dolphinscheduler.common.enums.StateEventType;
import org.apache.dolphinscheduler.common.utils.HttpUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.server.master.metrics.TaskMetrics;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable;
import org.apache.dolphinscheduler.server.master.runner.task.ITaskProcessor;
import org.apache.dolphinscheduler.server.master.runner.task.TaskAction;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

@AutoService(StateEventHandler.class)
public class TaskStateEventHandler implements StateEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(TaskStateEventHandler.class);

    private String makeTaskInstanceStateMsg(String workflowInstanceId, String dolphinSchedulerProcessInstanceId,
                                                String dolphinSchedulerTaskCode, String dolphinSchedulerTaskInstanceId, String status) {
        JsonObject jsonBody = new JsonObject();
        jsonBody.addProperty("workflowInstanceId", workflowInstanceId);
        jsonBody.addProperty("dolphinSchedulerProcessInstanceId", dolphinSchedulerProcessInstanceId);
        jsonBody.addProperty("dolphinSchedulerTaskCode", dolphinSchedulerTaskCode);
        jsonBody.addProperty("dolphinSchedulerTaskInstanceId", dolphinSchedulerTaskInstanceId);
        jsonBody.addProperty("status", status);
        return jsonBody.toString();
    }

    private void notifyTaskInstanceStateEvent(ProcessInstance processInstance, StateEvent event) {
        try {
            final String processGlobalParams = processInstance.getGlobalParams();

            if (processGlobalParams == null) {
                throw new RuntimeException("No global params for process instance id[" + processInstance.getId() + "]");
            }

            final JsonObject globalParams = new Gson().fromJson(processGlobalParams, JsonObject.class);

            String uri;
            if (globalParams.has("NDAP_WORKFLOW_INSTANCE_NOTIFICATION_URI")) {
                uri = globalParams.get("NDAP_WORKFLOW_INSTANCE_NOTIFICATION_URI").getAsString();
            } else {
                throw new RuntimeException("No NDAP_WORKFLOW_INSTANCE_NOTIFICATION_URI in process instance id[" + processInstance.getId() + "]");
            }

            String workflowInstanceId;
            if (globalParams.has("NDAP_SUB_WORKFLOW_PARENT_INSTANCE_ID")) {
                workflowInstanceId = globalParams.get("NDAP_SUB_WORKFLOW_PARENT_INSTANCE_ID").getAsString();
            } else {
                throw new RuntimeException("No NDAP_SUB_WORKFLOW_PARENT_INSTANCE_ID in process instance id[" + processInstance.getId() + "]");
            }

            logger.info("ndap workflow instance id[{}], process instance id[{}], task code[{}], task instance id[{}], notification uri[{}] process global params[{}]",
                    workflowInstanceId, event.getProcessInstanceId(), event.getTaskCode(), event.getTaskInstanceId(), uri, processGlobalParams);
            logger.info("ndap workflow instance id[{}], process instance id[{}], task code[{}], task instance id[{}], notification uri[{}] process var pool[{}]",
                    workflowInstanceId, event.getProcessInstanceId(), event.getTaskCode(), event.getTaskInstanceId(), uri, processInstance.getVarPool());

            String reqBody = makeTaskInstanceStateMsg(workflowInstanceId, String.valueOf(event.getProcessInstanceId()),
                    String.valueOf(event.getTaskCode()), String.valueOf(event.getTaskInstanceId()), event.getExecutionStatus().name());
            ByteArrayEntity entity = new ByteArrayEntity(reqBody.getBytes(StandardCharsets.UTF_8));

            HttpPost request = new HttpPost(uri);
            request.setHeader(HttpHeaders.CONTENT_TYPE, "application/json;utf-8");
            request.setEntity(entity);
            HttpUtils.getInstance().execute(request);
        } catch (Exception e) {
            logger.warn("----notifying task instance status failed : {}", e.getMessage());
        }
    }
    @Override
    public boolean handleStateEvent(WorkflowExecuteRunnable workflowExecuteRunnable, StateEvent stateEvent)
        throws StateEventHandleException, StateEventHandleError {
        measureTaskState(stateEvent);
        workflowExecuteRunnable.checkTaskInstanceByStateEvent(stateEvent);

        Optional<TaskInstance> taskInstanceOptional =
            workflowExecuteRunnable.getTaskInstance(stateEvent.getTaskInstanceId());

        TaskInstance task = taskInstanceOptional.orElseThrow(() -> new StateEventHandleError(
            "Cannot find task instance from taskMap by task instance id: " + stateEvent.getTaskInstanceId()));

        if (task.getState() == null) {
            throw new StateEventHandleError("Task state event handle error due to task state is null");
        }

        ProcessInstance processInstance = workflowExecuteRunnable.getProcessInstance();
        if (processInstance!=null) {
            notifyTaskInstanceStateEvent(processInstance, stateEvent);
        } else {
            logger.warn("---- process instance is null. process instance id[{}], task instance id[{}], task state[{}]",
                    stateEvent.getProcessInstanceId(), stateEvent.getTaskInstanceId(), task.getState().name());
        }

        Map<Long, Integer> completeTaskMap = workflowExecuteRunnable.getCompleteTaskMap();

        if (task.getState().typeIsFinished()) {
            if (completeTaskMap.containsKey(task.getTaskCode())
                && completeTaskMap.get(task.getTaskCode()) == task.getId()) {
                logger.warn("The task instance is already complete, stateEvent: {}", stateEvent);
                return true;
            }
            workflowExecuteRunnable.taskFinished(task);
            if (task.getTaskGroupId() > 0) {
                logger.info("The task instance need to release task Group: {}", task.getTaskGroupId());
                workflowExecuteRunnable.releaseTaskGroup(task);
            }
            return true;
        }
        Map<Long, ITaskProcessor> activeTaskProcessMap = workflowExecuteRunnable.getActiveTaskProcessMap();
        if (activeTaskProcessMap.containsKey(task.getTaskCode())) {
            ITaskProcessor iTaskProcessor = activeTaskProcessMap.get(task.getTaskCode());
            iTaskProcessor.action(TaskAction.RUN);

            if (iTaskProcessor.taskInstance().getState().typeIsFinished()) {
                if (iTaskProcessor.taskInstance().getState() != task.getState()) {
                    task.setState(iTaskProcessor.taskInstance().getState());
                }
                workflowExecuteRunnable.taskFinished(task);
            }
            return true;
        }
        throw new StateEventHandleError(
                "Task state event handle error, due to the task is not in activeTaskProcessorMaps");
    }

    @Override
    public StateEventType getEventType() {
        return StateEventType.TASK_STATE_CHANGE;
    }

    private void measureTaskState(StateEvent taskStateEvent) {
        if (taskStateEvent == null || taskStateEvent.getExecutionStatus() == null) {
            // the event is broken
            logger.warn("The task event is broken..., taskEvent: {}", taskStateEvent);
            return;
        }
        if (taskStateEvent.getExecutionStatus().typeIsFinished()) {
            TaskMetrics.incTaskFinish();
        }
        switch (taskStateEvent.getExecutionStatus()) {
            case STOP:
                TaskMetrics.incTaskStop();
                break;
            case SUCCESS:
                TaskMetrics.incTaskSuccess();
                break;
            case FAILURE:
                TaskMetrics.incTaskFailure();
                break;
            default:
                break;
        }
    }
}
