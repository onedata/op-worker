-include("appmock.hrl").
-include("client_interface.hrl").

-define(APP_NAME, appmock).

-record(mapping_state, {
    response = #mock_resp{} :: #mock_resp{} | function(),
    % Used to remember state between requests on the same stub
    state = []
}).

