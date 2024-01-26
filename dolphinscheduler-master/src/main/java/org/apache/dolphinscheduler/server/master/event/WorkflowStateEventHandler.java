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

import com.google.auto.service.AutoService;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.common.enums.StateEventType;
import org.apache.dolphinscheduler.common.utils.HttpUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.server.master.metrics.ProcessInstanceMetrics;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;

import java.nio.charset.StandardCharsets;

@AutoService(StateEventHandler.class)
@Slf4j
public class WorkflowStateEventHandler implements StateEventHandler {

    private String makeWorkflowInstanceStateMsg(String workflowInstanceId, String dolphinSchedulerProcessInstanceId,
                                                String dolphinSchedulerTaskCode, String dolphinSchedulerTaskInstanceId, String status) {
        JsonObject jsonBody = new JsonObject();
        jsonBody.addProperty("workflowInstanceId", workflowInstanceId);
        jsonBody.addProperty("dolphinSchedulerProcessInstanceId", dolphinSchedulerProcessInstanceId);
        jsonBody.addProperty("dolphinSchedulerTaskCode", dolphinSchedulerTaskCode);
        jsonBody.addProperty("dolphinSchedulerTaskInstanceId", dolphinSchedulerTaskInstanceId);
        jsonBody.addProperty("status", status);
        return jsonBody.toString();
    }

    private void notifyWorkflowInstanceStateEvent(ProcessInstance processInstance, WorkflowStateEvent event) {
        try {
            final String processGlobalParams = processInstance.getGlobalParams();

            if (processGlobalParams == null) {
                throw new RuntimeException("No global params for process instance id[" + processInstance.getId() + "]");
            }

            final JsonObject globalParams = new Gson().fromJson(processGlobalParams, JsonObject.class);

            log.info("event key[{}], status[{}], process instance id[{}], task instance id[{}], process global params[{}]",
                    event.getKey(), event.getStatus().name(), event.getProcessInstanceId(),
                    event.getTaskInstanceId(), processGlobalParams);

            log.info("event key[{}], status[{}], process instance id[{}], task instance id[{}], process var pool[{}]",
                    event.getKey(), event.getStatus().name(), event.getProcessInstanceId(),
                    event.getTaskInstanceId(), processInstance.getVarPool());

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

            log.info("event key[{}], status[{}], ndap workflow instance id[{}], process instance id[{}], task instance id[{}], notification uri[{}]",
                    event.getKey(), event.getStatus().name(), workflowInstanceId, event.getProcessInstanceId(),
                    event.getTaskInstanceId(), uri);

            String reqBody = makeWorkflowInstanceStateMsg(workflowInstanceId, String.valueOf(event.getProcessInstanceId()),
                    null, String.valueOf(event.getTaskInstanceId()), event.getStatus().name());
            ByteArrayEntity entity = new ByteArrayEntity(reqBody.getBytes(StandardCharsets.UTF_8));

            HttpPost request = new HttpPost(uri);
            request.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
            request.setEntity(entity);
            HttpUtils.getInstance().execute(request);
        } catch (Exception e) {
            log.warn("----notifying workflow instance status failed : {}", e.getMessage());
        }
    }

    @Override
    public boolean handleStateEvent(WorkflowExecuteRunnable workflowExecuteRunnable,
                                    StateEvent stateEvent) throws StateEventHandleException {
        WorkflowStateEvent workflowStateEvent = (WorkflowStateEvent) stateEvent;
        ProcessInstance processInstance =
                workflowExecuteRunnable.getWorkflowExecuteContext().getWorkflowInstance();
        ProcessDefinition processDefinition = processInstance.getProcessDefinition();
        measureProcessState(workflowStateEvent, processInstance.getProcessDefinitionCode().toString());

        log.info(
                "Handle workflow instance state event, the current workflow instance state {} will be changed to {}",
                processInstance.getState(), workflowStateEvent.getStatus());

        //To NDAP
        notifyWorkflowInstanceStateEvent(workflowExecuteRunnable.getWorkflowExecuteContext().getWorkflowInstance(), workflowStateEvent);

        if (workflowStateEvent.getStatus().isStop()) {
            // serial wait execution type needs to wake up the waiting process
            if (processDefinition.getExecutionType().typeIsSerialWait() || processDefinition.getExecutionType()
                    .typeIsSerialPriority()) {
                workflowExecuteRunnable.endProcess();
                return true;
            }
            workflowExecuteRunnable.updateProcessInstanceState(workflowStateEvent);
            return true;
        }
        if (workflowExecuteRunnable.processComplementData()) {
            return true;
        }
        if (workflowStateEvent.getStatus().isFinished()) {
            if (workflowStateEvent.getType().equals(StateEventType.PROCESS_SUBMIT_FAILED)) {
                workflowExecuteRunnable.updateProcessInstanceState(workflowStateEvent);
            }
            workflowExecuteRunnable.endProcess();
        }
        if (processInstance.getState().isReadyStop()) {
            workflowExecuteRunnable.killAllTasks();
        }

        return true;
    }

    @Override
    public StateEventType getEventType() {
        return StateEventType.PROCESS_STATE_CHANGE;
    }

    private void measureProcessState(WorkflowStateEvent processStateEvent, String processDefinitionCode) {
        if (processStateEvent.getStatus().isFinished()) {
            ProcessInstanceMetrics.incProcessInstanceByStateAndProcessDefinitionCode("finish", processDefinitionCode);
        }
        switch (processStateEvent.getStatus()) {
            case STOP:
                ProcessInstanceMetrics.incProcessInstanceByStateAndProcessDefinitionCode("stop", processDefinitionCode);
                break;
            case SUCCESS:
                ProcessInstanceMetrics.incProcessInstanceByStateAndProcessDefinitionCode("success",
                        processDefinitionCode);
                break;
            case FAILURE:
                ProcessInstanceMetrics.incProcessInstanceByStateAndProcessDefinitionCode("fail", processDefinitionCode);
                break;
            default:
                break;
        }
    }
}
