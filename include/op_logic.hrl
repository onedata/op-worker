%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO VFS-5621
%%% Common definitions concerning op logic.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(OP_LOGIC_HRL).
-define(OP_LOGIC_HRL, 1).

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").

% Record expressing op logic request
-record(op_req, {
    auth = ?NOBODY :: aai:auth(),
    gri :: op_logic:gri(),
    operation = create :: op_logic:operation(),
    data = #{} :: op_logic:data(),
    auth_hint = undefined :: undefined | op_logic:auth_hint()
}).

-endif.
