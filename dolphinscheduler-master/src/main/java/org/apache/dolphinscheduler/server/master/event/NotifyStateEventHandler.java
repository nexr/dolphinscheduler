package org.apache.dolphinscheduler.server.master.event;

import com.google.auto.service.AutoService;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.dolphinscheduler.common.enums.StateEventType;
import org.apache.dolphinscheduler.common.utils.HttpUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;
import java.util.Optional;

@AutoService(StateEventHandler.class)
public class NotifyStateEventHandler implements StateEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(NotifyStateEventHandler.class);
    private static final String NDAP_WORKFLOW_INSTANCE_ID_PROP = "NDAP_SUB_WORKFLOW_PARENT_INSTANCE_ID";
    private static final String NDAP_WORKFLOW_INSTANCE_STATE_CHANGE_URI_PROP = "NDAP_WORKFLOW_INSTANCE_NOTIFICATION_URI";
    private static final String NDAP_TASK_INSTANCE_STATE_CHANGE_URI_PROP = "NDAP_TASK_INSTANCE_NOTIFICATION_URI";
    private String makeStateEventMsg(Long workflowInstanceId, Long dolphinSchedulerProcessInstanceId,
                                     Long dolphinSchedulerTaskCode, Long dolphinSchedulerTaskInstanceId,
                                     String status, String taskResult, Date startTime, Date endTime) {
        JsonObject jsonBody = new JsonObject();
        jsonBody.addProperty("workflowInstanceId", workflowInstanceId);
        jsonBody.addProperty("dolphinSchedulerProcessInstanceId", dolphinSchedulerProcessInstanceId);
        jsonBody.addProperty("dolphinSchedulerTaskCode", dolphinSchedulerTaskCode);
        jsonBody.addProperty("dolphinSchedulerTaskInstanceId", dolphinSchedulerTaskInstanceId);
        jsonBody.addProperty("status", status);
        jsonBody.addProperty("taskResult", taskResult);
        jsonBody.addProperty("startTime", (startTime != null) ? startTime.getTime() : null);
        jsonBody.addProperty("endTime", (endTime != null) ? endTime.getTime() : null);
        return jsonBody.toString();
    }

    private String findOutValueFromVarPool(JsonArray pool) {
        if (pool == null || pool.isEmpty()) {
            return null;
        }

        Iterator<JsonElement> iter = pool.iterator();

        while (iter.hasNext()) {
            JsonObject prop = iter.next().getAsJsonObject();
            if (prop.get("direct").getAsString().equals("OUT")) {
                return String.format("%s=%s", prop.get("prop").getAsString(), prop.get("value").getAsString());
            }
        }

        return null;
    }

    private String findValueFromGlobalParams(JsonArray params, String propValue) {
        if (params == null ||  params.isEmpty()) {
            return null;
        }

        Iterator<JsonElement> iter = params.iterator();

        while (iter.hasNext()) {
            JsonObject prop = iter.next().getAsJsonObject();
            if (prop.get("prop").getAsString().equals(propValue)) {
                return prop.get("value").getAsString();
            }
        }

       return null;
    }

    private boolean notifyWorkflowStateEvent(WorkflowExecuteRunnable workflowExecuteRunnable, StateEvent stateEvent) {
        try {
            final ProcessInstance processInstance = workflowExecuteRunnable.getProcessInstance();

            if (processInstance == null) {
                logger.warn("process instance is null. event type[{}], process instance id[{}], task instance id[{}], state[{}]",
                        stateEvent.getType().name(), stateEvent.getProcessInstanceId(), stateEvent.getTaskInstanceId(), stateEvent.getExecutionStatus().name());
                return false;
            }

            String processGlobalParams = processInstance.getGlobalParams();

            if (processGlobalParams == null) {
                logger.warn("No global params for process instance id[" + processInstance.getId() + "]");
                return false;
            }

            final JsonArray globalParams = new Gson().fromJson(processGlobalParams, JsonArray.class);
            final String workflowInstanceId = findValueFromGlobalParams(globalParams, NDAP_WORKFLOW_INSTANCE_ID_PROP);

            if (workflowInstanceId == null) {
                logger.warn("No NDAP_SUB_WORKFLOW_PARENT_INSTANCE_ID in process instance id[{}]", processInstance.getId());
                return false;
            }

            final String uri = findValueFromGlobalParams(globalParams, NDAP_WORKFLOW_INSTANCE_STATE_CHANGE_URI_PROP);

            if (uri == null) {
                logger.warn("No notification url for state event type[{}]", stateEvent.getType().name());
                return false;
            }

            final String reqBody = makeStateEventMsg(Long.valueOf(workflowInstanceId), (long)stateEvent.getProcessInstanceId(),
                    null, null, stateEvent.getExecutionStatus().name(),
                    null, processInstance.getStartTime(), processInstance.getEndTime());

            logger.info("notifying state change event to NDAP. url[{}], msg[{}]", uri, reqBody);

            sendEventMsg(uri, reqBody);
            return true;
        } catch (Exception e) {
            logger.warn("notifying workflow instance status failed : {}", e.getMessage());
        }
        return false;
    }

    private boolean notifyTaskStateEvent(WorkflowExecuteRunnable workflowExecuteRunnable, StateEvent stateEvent) {
        try {
            final ProcessInstance processInstance = workflowExecuteRunnable.getProcessInstance();

            if (processInstance == null) {
                logger.warn("process instance is null. event type[{}], process instance id[{}], task instance id[{}], state[{}]",
                        stateEvent.getType().name(), stateEvent.getProcessInstanceId(), stateEvent.getTaskInstanceId(), stateEvent.getExecutionStatus().name());
                return false;
            }

            String processGlobalParams = processInstance.getGlobalParams();

            if (processGlobalParams == null) {
                logger.warn("No global params for process instance id[" + processInstance.getId() + "]");
                return false;
            }

            final JsonArray globalParams = new Gson().fromJson(processGlobalParams, JsonArray.class);
            final String workflowInstanceId = findValueFromGlobalParams(globalParams, NDAP_WORKFLOW_INSTANCE_ID_PROP);

            if (workflowInstanceId == null) {
                logger.warn("No NDAP_SUB_WORKFLOW_PARENT_INSTANCE_ID in process instance id[{}]", processInstance.getId());
                return false;
            }

            Optional<TaskInstance> taskInstanceOptional =
                    workflowExecuteRunnable.getTaskInstance(stateEvent.getTaskInstanceId());

            final TaskInstance taskInstance = taskInstanceOptional.orElse(null);

            if (taskInstance == null) {
                logger.warn("Task state event handle error due to task instance is null");
                return false;
            }

            if (taskInstance.getState() == null) {
                logger.warn("Task state event handle error due to task instance state is null");
                return false;
            }

            String uri = findValueFromGlobalParams(globalParams, NDAP_TASK_INSTANCE_STATE_CHANGE_URI_PROP);

            if (uri == null) {
                logger.warn("No notification url for state event type[{}]", stateEvent.getType().name());
                return false;
            }

            String taskResult = null;

            if (taskInstance.getState().typeIsSuccess() && taskInstance.getVarPool() != null) {
                final JsonArray taskVarPool = new Gson().fromJson(taskInstance.getVarPool(), JsonArray.class);
                taskResult = findOutValueFromVarPool(taskVarPool);
            }

            final String reqBody = makeStateEventMsg(Long.valueOf(workflowInstanceId), (long)stateEvent.getProcessInstanceId(),
                    taskInstance.getTaskCode(), (long)taskInstance.getId(), taskInstance.getState().name(),
                    taskResult, taskInstance.getStartTime(), taskInstance.getEndTime());

            logger.info("notifying state change event to NDAP. url[{}], msg[{}]", uri, reqBody);
            sendEventMsg(uri, reqBody);
            return true;
        } catch (Exception e) {
            logger.warn("notifying workflow instance status failed : {}", e.getMessage());
        }
        return false;
    }

    private void sendEventMsg (String uri, String msg) throws Exception {
        HttpPost request = new HttpPost(uri);
        request.setEntity(new StringEntity(msg, ContentType.APPLICATION_JSON));
        HttpResponse response = HttpUtils.getInstance().execute(request);
        logger.info("notify to {} with msg {} status line {}", uri, msg, response.getStatusLine().toString());
    }

    @Override
    public boolean handleStateEvent(WorkflowExecuteRunnable workflowExecuteRunnable, StateEvent stateEvent) throws StateEventHandleException, StateEventHandleError {
        switch (stateEvent.getType()) {
            case PROCESS_STATE_CHANGE:
                return notifyWorkflowStateEvent(workflowExecuteRunnable, stateEvent);
            case TASK_STATE_CHANGE:
                return notifyTaskStateEvent(workflowExecuteRunnable, stateEvent);
            default:
                return false;
        }
    }

    @Override
    public StateEventType getEventType() {
        return StateEventType.NOTIFY_STATE_CHANGE;
    }
}
