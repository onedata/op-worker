

-module(mock_app_behaviour).

-include("appmock.hrl").

-callback request_mappings() -> [#mapping{}].