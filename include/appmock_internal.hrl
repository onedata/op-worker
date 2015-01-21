-include("appmock.hrl").

-define(APP_NAME, appmock).

-define(REMOTE_CONTROL_VERIFY_PATH, "/verify").

-record(mapping_state, {
    response = #mock_resp{} :: #mock_resp{} | function(),
    % Used to remember state between requests on the same stub
    state = []
}).