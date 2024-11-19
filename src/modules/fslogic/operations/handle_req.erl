%%%--------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests modifying handles.
%%% @end
%%%--------------------------------------------------------------------
-module(handle_req).
-author("Tomasz Lichon").

%% API
-export([create_handle/6, get_handle/2]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @equiv create_share_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec create_handle(
    user_ctx:ctx(), file_ctx:ctx(), od_share:id(), od_handle_service:id(),
    od_handle:metadata_prefix(), od_handle:metadata()
) ->
    {ok, od_handle:id()} | no_return().
create_handle(UserCtx, FileCtx, ShareId, HServiceId, MetadataPrefix, MetadataString) ->
    file_ctx:assert_not_trash_or_tmp_dir_const(FileCtx),
    data_constraints:assert_not_readonly_mode(UserCtx),

    SessionId = user_ctx:get_session_id(UserCtx),
    handle_logic:create(SessionId, HServiceId, <<"Share">>, ShareId, MetadataPrefix, MetadataString).


-spec get_handle(user_ctx:ctx(), od_handle:id()) -> {ok, od_handle:doc()} | no_return().
get_handle(UserCtx, HandleId) ->
    data_constraints:assert_not_readonly_mode(UserCtx),

    SessionId = user_ctx:get_session_id(UserCtx),
    handle_logic:get(SessionId, HandleId).
