

-module(mock_app_description).

-include("appmock.hrl").

-callback request_mappings() -> [#mapping{}].