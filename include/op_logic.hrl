%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions concerning entity logic.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(OP_LOGIC_HRL).
-define(OP_LOGIC_HRL, 1).

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").

% Record expressing entity logic request client (REST and Graph Sync).
-record(client, {
    % root is allowed to do anything, it must be used with caution
    % (should not be used in any kind of external API!)
    type = nobody :: user | root | nobody,
    id :: binary(),
    session_id :: session:id()
}).

% Record expressing entity logic request
-record(op_req, {
    client = #client{} :: op_logic:client(),
    gri :: op_logic:gri(),
    operation = create :: op_logic:operation(),
    data = #{} :: op_logic:data(),
    auth_hint = undefined :: undefined | op_logic:auth_hint()
}).

% Convenience macros for concise code
-define(USER, #client{type = user}).
-define(USER(__Id), #client{
    type = user,
    id = __Id
}).
-define(USER(__Id, __SessionId), #client{
    type = user,
    id = __Id,
    session_id = __SessionId
}).
-define(ROOT, #client{
    type = root,
    id = ?ROOT_SESS_ID,
    session_id = ?ROOT_SESS_ID
}).
-define(NOBODY, #client{
    type = nobody,
    id = ?GUEST_SESS_ID,
    session_id = ?GUEST_SESS_ID
}).

-endif.
