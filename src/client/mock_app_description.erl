

-module(mock_app_description).

-include("appmock.hrl").

-callback response_mocks() -> [#mock_resp_mapping{}].