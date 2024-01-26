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
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.Date;
import java.util.Iterator;
import java.util.Optional;


@Slf4j
@AutoService(StateEventHandler.class)
public class NotifyStateEventHandler implements StateEventHandler {
    private static final String NDAP_WORKFLOW_INSTANCE_ID_PROP = "NDAP_SUB_WORKFLOW_PARENT_INSTANCE_ID";
    private static final String NDAP_WORKFLOW_INSTANCE_STATE_CHANGE_URI_PROP = "NDAP_WORKFLOW_INSTANCE_NOTIFICATION_URI";
    private static final String NDAP_TASK_INSTANCE_STATE_CHANGE_URI_PROP = "NDAP_TASK_INSTANCE_NOTIFICATION_URI";
    private static final String NDAP_CONTROL_NODE_NAME_PREFIX = "%s_%s";

    private String makeStateEventMsg(Long workflowInstanceId, Long dolphinSchedulerProcessInstanceId,
                                     Long dolphinSchedulerTaskCode, Long dolphinSchedulerTaskInstanceId,
                                     String status, Date startTime, Date endTime) {
        JsonObject jsonBody = new JsonObject();
        jsonBody.addProperty("workflowInstanceId", workflowInstanceId);
        jsonBody.addProperty("dolphinSchedulerProcessInstanceId", dolphinSchedulerProcessInstanceId);
        jsonBody.addProperty("dolphinSchedulerTaskCode", dolphinSchedulerTaskCode);
        jsonBody.addProperty("dolphinSchedulerTaskInstanceId", dolphinSchedulerTaskInstanceId);
        jsonBody.addProperty("status", status);
        jsonBody.addProperty("startTime", (startTime != null) ? startTime.getTime() : null);
        jsonBody.addProperty("endTime", (endTime != null) ? endTime.getTime() : null);
        return jsonBody.toString();
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

    private boolean notifyWorkflowStateEvent(WorkflowExecuteRunnable workflowExecuteRunnable, WorkflowStateEvent stateEvent) {
        try {
            final ProcessInstance processInstance = workflowExecuteRunnable.getWorkflowExecuteContext().getWorkflowInstance();

            if (processInstance == null) {
                log.warn("process instance is null. event type[{}], process instance id[{}], task instance id[{}], state[{}]",
                        stateEvent.getType().name(), stateEvent.getProcessInstanceId(), stateEvent.getTaskInstanceId(), stateEvent.getStatus().name());
                return false;
            }

            String processGlobalParams = processInstance.getGlobalParams();

            if (processGlobalParams == null) {
                log.warn("No global params for process instance id[" + processInstance.getId() + "]");
                return false;
            }

            final JsonArray globalParams = new Gson().fromJson(processGlobalParams, JsonArray.class);
            final String workflowInstanceId = findValueFromGlobalParams(globalParams, NDAP_WORKFLOW_INSTANCE_ID_PROP);

            if (workflowInstanceId == null) {
                log.warn("No NDAP_SUB_WORKFLOW_PARENT_INSTANCE_ID in process instance id[{}]", processInstance.getId());
                return false;
            }

            final String uri = findValueFromGlobalParams(globalParams, NDAP_WORKFLOW_INSTANCE_STATE_CHANGE_URI_PROP);

            if (uri == null) {
                log.warn("No notification url for state event type[{}]", stateEvent.getType().name());
                return false;
            }

            final String reqBody = makeStateEventMsg(Long.valueOf(workflowInstanceId), (long)stateEvent.getProcessInstanceId(),
                    null, null, stateEvent.getStatus().name(),
                    processInstance.getStartTime(), processInstance.getEndTime());

            sendEventMsg(uri, reqBody);
            return true;
        } catch (Exception e) {
            log.warn("notifying workflow instance status failed : {}", e.getMessage());
        }
        return false;
    }

    private boolean isControlNode(TaskInstance taskInstance) {
        //for SWITCH(Decision) node
        if (!taskInstance.getTaskType().equals("SHELL")) {
            return true;
        }

        final String taskName = taskInstance.getName();

        if (taskName.startsWith(String.format(NDAP_CONTROL_NODE_NAME_PREFIX, "join", taskInstance.getTaskCode()))) {
            return true;
        }

        if (taskName.startsWith(String.format(NDAP_CONTROL_NODE_NAME_PREFIX, "fork", taskInstance.getTaskCode()))) {
            return true;
        }

        return false;
    }

    private boolean notifyTaskStateEvent(WorkflowExecuteRunnable workflowExecuteRunnable, TaskStateEvent stateEvent) {
        try {

            Optional<TaskInstance> taskInstanceOptional =
                    workflowExecuteRunnable.getTaskInstance(stateEvent.getTaskInstanceId());
            final TaskInstance taskInstance = taskInstanceOptional.orElse(null);

            if (taskInstance == null) {
                log.warn("Task state event handle error due to task instance is null");
                return false;
            }

            if (taskInstance.getState() == null) {
                log.warn("Task state event handle error due to task instance state is null");
                return false;
            }

            if (isControlNode(taskInstance)){
                log.debug("Event for Ndap control node : no need to notify.");
                return false;
            }

            final ProcessInstance processInstance = workflowExecuteRunnable.getWorkflowExecuteContext().getWorkflowInstance();

            if (processInstance == null) {
                log.warn("process instance is null. event type[{}], process instance id[{}], task instance id[{}], state[{}]",
                        stateEvent.getType().name(), stateEvent.getProcessInstanceId(), stateEvent.getTaskInstanceId(), stateEvent.getStatus().name());
                return false;
            }

            final String processGlobalParams = processInstance.getGlobalParams();

            if (processGlobalParams == null) {
                log.warn("No global params for process instance id[" + processInstance.getId() + "]");
                return false;
            }

            final JsonArray globalParams = new Gson().fromJson(processGlobalParams, JsonArray.class);
            final String workflowInstanceId = findValueFromGlobalParams(globalParams, NDAP_WORKFLOW_INSTANCE_ID_PROP);

            if (workflowInstanceId == null) {
                log.warn("No NDAP_SUB_WORKFLOW_PARENT_INSTANCE_ID in process instance id[{}]", processInstance.getId());
                return false;
            }

            final String uri = findValueFromGlobalParams(globalParams, NDAP_TASK_INSTANCE_STATE_CHANGE_URI_PROP);

            if (uri == null) {
                log.warn("No notification url for state event type[{}]", stateEvent.getType().name());
                return false;
            }

            final String reqBody = makeStateEventMsg(Long.valueOf(workflowInstanceId), (long)stateEvent.getProcessInstanceId(),
                    taskInstance.getTaskCode(), (long)taskInstance.getId(), taskInstance.getState().name(),
                    taskInstance.getStartTime(), taskInstance.getEndTime());

            log.info("notifying state change event to NDAP. url[{}], msg[{}]", uri, reqBody);
            sendEventMsg(uri, reqBody);
            return true;
        } catch (Exception e) {
            log.warn("notifying workflow instance status failed : {}", e.getMessage());
        }
        return false;
    }

    private void sendEventMsg (String uri, String msg) throws Exception {
        HttpPost request = new HttpPost(uri);
        request.setEntity(new StringEntity(msg, ContentType.APPLICATION_JSON));

        try (CloseableHttpResponse response = HttpUtils.getInstance().execute(request)) {
            log.info("notify to {} with msg {} status line : {}", uri, msg, response.getStatusLine().toString());

            if (response != null) {
                EntityUtils.consumeQuietly(response.getEntity());
            }
        }

        request.releaseConnection();
    }

    @Override
    public boolean handleStateEvent(WorkflowExecuteRunnable workflowExecuteRunnable, StateEvent stateEvent) throws StateEventHandleException, StateEventHandleError {
        switch (stateEvent.getType()) {
            case PROCESS_STATE_CHANGE:
                return notifyWorkflowStateEvent(workflowExecuteRunnable, (WorkflowStateEvent)stateEvent);
            case TASK_STATE_CHANGE:
                return notifyTaskStateEvent(workflowExecuteRunnable, (TaskStateEvent)stateEvent);
            default:
                return false;
        }
    }

    @Override
    public StateEventType getEventType() {
        return StateEventType.NOTIFY_STATE_CHANGE;
    }
}
