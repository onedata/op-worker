%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: @TODO: write me
%% @end
%% ===================================================================

-ifndef(GATEWAY_HRL).
-define(GATEWAY_HRL, true).

-define(gw_port, 8877).

-record(file_chunk, {
    file_path :: binary(),
    offset = 0 :: non_neg_integer(),
    size = eof :: pos_integer() | eof
}).

-endif.
