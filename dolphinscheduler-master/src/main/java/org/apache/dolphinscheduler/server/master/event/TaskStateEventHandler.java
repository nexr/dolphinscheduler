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

import org.apache.dolphinscheduler.common.enums.StateEventType;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.remote.exceptions.RemotingException;
import org.apache.dolphinscheduler.server.master.metrics.TaskMetrics;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable;

import java.util.Optional;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import com.google.auto.service.AutoService;

@AutoService(StateEventHandler.class)
@Slf4j
public class TaskStateEventHandler implements StateEventHandler {

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

            log.info("process instance id[{}], task code[{}], task instance id[{}], process global params[{}]",
                    event.getProcessInstanceId(), event.getTaskCode(), event.getTaskInstanceId(), processGlobalParams);

            log.info("process instance id[{}], task code[{}], task instance id[{}], process var pool[{}]",
                    event.getProcessInstanceId(), event.getTaskCode(), event.getTaskInstanceId(), processInstance.getVarPool());

            String uri;
            if (globalParams.has("NDAP_TASK_INSTANCE_NOTIFICATION_URI")) {
                uri = globalParams.get("NDAP_TASK_INSTANCE_NOTIFICATION_URI").getAsString();
            } else {
                throw new RuntimeException("No NDAP_TASK_INSTANCE_NOTIFICATION_URI in process instance id[" + processInstance.getId() + "]");
            }

            String workflowInstanceId;
            if (globalParams.has("NDAP_SUB_WORKFLOW_PARENT_INSTANCE_ID")) {
                workflowInstanceId = globalParams.get("NDAP_SUB_WORKFLOW_PARENT_INSTANCE_ID").getAsString();
            } else {
                throw new RuntimeException("No NDAP_SUB_WORKFLOW_PARENT_INSTANCE_ID in process instance id[" + processInstance.getId() + "]");
            }

            log.info("ndap workflow instance id[{}], process instance id[{}], task code[{}], task instance id[{}], notification uri[{}]",
                    workflowInstanceId, event.getProcessInstanceId(), event.getTaskCode(), event.getTaskInstanceId(), uri);

            String reqBody = makeTaskInstanceStateMsg(workflowInstanceId, String.valueOf(event.getProcessInstanceId()),
                    String.valueOf(event.getTaskCode()), String.valueOf(event.getTaskInstanceId()), event.getExecutionStatus().name());
            ByteArrayEntity entity = new ByteArrayEntity(reqBody.getBytes(StandardCharsets.UTF_8));

            HttpPost request = new HttpPost(uri);
            request.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
            request.setEntity(entity);
            HttpUtils.getInstance().execute(request);
        } catch (Exception e) {
            log.warn("----notifying task instance status failed : {}", e.getMessage());
        }
    }

    @Override
    public boolean handleStateEvent(WorkflowExecuteRunnable workflowExecuteRunnable,
                                    StateEvent stateEvent) throws StateEventHandleException, StateEventHandleError {
        TaskStateEvent taskStateEvent = (TaskStateEvent) stateEvent;
        measureTaskState(taskStateEvent);
        workflowExecuteRunnable.checkTaskInstanceByStateEvent(taskStateEvent);

        Optional<TaskInstance> taskInstanceOptional =
                workflowExecuteRunnable.getTaskInstance(taskStateEvent.getTaskInstanceId());

        TaskInstance task = taskInstanceOptional.orElseThrow(() -> new StateEventHandleError(
                "Cannot find task instance from taskMap by task instance id: " + taskStateEvent.getTaskInstanceId()));

        if (task.getState() == null) {
            throw new StateEventHandleError("Task state event handle error due to task state is null");
        }

        ProcessInstance processInstance = workflowExecuteRunnable.getProcessInstance();
        if (processInstance!=null) {
            notifyTaskInstanceStateEvent(processInstance, stateEvent);
        } else {
            log.warn("---- process instance is null. process instance id[{}], task instance id[{}], task state[{}]",
                    stateEvent.getProcessInstanceId(), stateEvent.getTaskInstanceId(), task.getState().name());
        }

        log.info(
                "Handle task instance state event, the current task instance state {} will be changed to {}",
                task.getState().name(), taskStateEvent.getStatus().name());

        Set<Long> completeTaskSet = workflowExecuteRunnable.getCompleteTaskCodes();
        if (task.getState().isFinished()
                && (taskStateEvent.getStatus() != null && taskStateEvent.getStatus().isRunning())) {
            String errorMessage = String.format(
                    "The current task instance state is %s, but the task state event status is %s, so the task state event will be ignored",
                    task.getState().name(),
                    taskStateEvent.getStatus().name());
            log.warn(errorMessage);
            throw new StateEventHandleError(errorMessage);
        }

        if (task.getState().isFinished()) {
            if (completeTaskSet.contains(task.getTaskCode())) {
                log.warn("The task instance is already complete, stateEvent: {}", stateEvent);
                return true;
            }
            workflowExecuteRunnable.taskFinished(task);
            if (task.getTaskGroupId() > 0) {
                log.info("The task instance need to release task Group: {}", task.getTaskGroupId());
                try {
                    workflowExecuteRunnable.releaseTaskGroup(task);
                } catch (RemotingException | InterruptedException e) {
                    throw new StateEventHandleException("Release task group failed", e);
                }
            }
            return true;
        }
        return true;
    }

    @Override
    public StateEventType getEventType() {
        return StateEventType.TASK_STATE_CHANGE;
    }

    private void measureTaskState(TaskStateEvent taskStateEvent) {
        if (taskStateEvent == null || taskStateEvent.getStatus() == null) {
            // the event is broken
            log.warn("The task event is broken..., taskEvent: {}", taskStateEvent);
            return;
        }
        if (taskStateEvent.getStatus().isFinished()) {
            TaskMetrics.incTaskInstanceByState("finish");
        }
        switch (taskStateEvent.getStatus()) {
            case KILL:
                TaskMetrics.incTaskInstanceByState("stop");
                break;
            case SUCCESS:
                TaskMetrics.incTaskInstanceByState("success");
                break;
            case FAILURE:
                TaskMetrics.incTaskInstanceByState("fail");
                break;
            default:
                break;
        }
    }
}
