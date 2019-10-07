%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Utils functions fon operating on transfer record.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_utils).
-author("Jakub Kudzia").

-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/privileges.hrl").

%% API
-export([
    authorize_creation/4,
    validate_creation/5,
    encode_pid/1, decode_pid/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec authorize_creation(od_space:id(), od_user:id(), transfer:type(),
    TransferByIndex :: boolean()) -> boolean().
authorize_creation(SpaceId, UserId, replication, false) ->
    RequiredPrivs = [?SPACE_SCHEDULE_REPLICATION],
    space_logic:has_eff_privileges(SpaceId, UserId, RequiredPrivs);
authorize_creation(SpaceId, UserId, replication, true) ->
    RequiredPrivs = [?SPACE_SCHEDULE_REPLICATION, ?SPACE_QUERY_VIEWS],
    space_logic:has_eff_privileges(SpaceId, UserId, RequiredPrivs);
authorize_creation(SpaceId, UserId, eviction, false) ->
    RequiredPrivs = [?SPACE_SCHEDULE_EVICTION],
    space_logic:has_eff_privileges(SpaceId, UserId, RequiredPrivs);
authorize_creation(SpaceId, UserId, eviction, true) ->
    RequiredPrivs = [?SPACE_SCHEDULE_EVICTION, ?SPACE_QUERY_VIEWS],
    space_logic:has_eff_privileges(SpaceId, UserId, RequiredPrivs);
authorize_creation(SpaceId, UserId, migration, false) ->
    RequiredPrivs = [?SPACE_SCHEDULE_REPLICATION, ?SPACE_SCHEDULE_EVICTION],
    space_logic:has_eff_privileges(SpaceId, UserId, RequiredPrivs);
authorize_creation(SpaceId, UserId, migration, true) ->
    RequiredPrivs = [
        ?SPACE_SCHEDULE_REPLICATION,
        ?SPACE_SCHEDULE_EVICTION,
        ?SPACE_QUERY_VIEWS
    ],
    space_logic:has_eff_privileges(SpaceId, UserId, RequiredPrivs).


%%--------------------------------------------------------------------
%% @doc
%% Validates whether transfer can be created/scheduled. That includes:
%% - check whether space is supported locally (scheduling provider)
%%   and by replicating and evicting providers,
%% - check if file exists in case of transfer of file/dir,
%% - check if index exists on replicating and evicting providers
%%   in case of transfer by index.
%% Depending on transfer type (replication, eviction, migration) either
%% ReplicatingProvider or EvictingProvider should be left 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec validate_creation(
    Auth :: aai:auth(),
    SpaceId :: od_space:id(),
    ReplicatingProvider :: undefined | od_provider:id(),
    EvictingProvider :: undefined | od_provider:id(),
    TransferBy :: {guid, file_id:file_guid()} | {view, index:name()}
) ->
    ok | no_return().
validate_creation(Auth, SpaceId, ReplicatingProvider, EvictingProvider, {guid, Guid}) ->
    op_logic_utils:assert_space_supported_locally(SpaceId),
    op_logic_utils:assert_file_exists(Auth, Guid),

    assert_space_supported_by(SpaceId, ReplicatingProvider),
    assert_space_supported_by(SpaceId, EvictingProvider);
validate_creation(_Auth, SpaceId, ReplicatingProvider, EvictingProvider, {view, Name}) ->
    op_logic_utils:assert_space_supported_locally(SpaceId),

    assert_space_supported_by(SpaceId, ReplicatingProvider),
    assert_view_exists_on_provider(SpaceId, Name, ReplicatingProvider),

    assert_space_supported_by(SpaceId, EvictingProvider),
    assert_view_exists_on_provider(SpaceId, Name, EvictingProvider).


-spec encode_pid(pid()) -> binary().
encode_pid(Pid) ->
    % todo remove after VFS-3657
    list_to_binary(pid_to_list(Pid)).


-spec decode_pid(binary()) -> pid().
decode_pid(Pid) ->
    % todo remove after VFS-3657
    list_to_pid(binary_to_list(Pid)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec assert_space_supported_by(od_space:id(), undefined | od_provider:id()) ->
    ok | no_return().
assert_space_supported_by(_SpaceId, undefined) ->
    ok;
assert_space_supported_by(SpaceId, ProviderId) ->
    op_logic_utils:assert_space_supported_by(SpaceId, ProviderId).


%% @private
-spec assert_view_exists_on_provider(od_space:id(), index:name(),
    undefined | od_provider:id()) -> ok | no_return().
assert_view_exists_on_provider(_SpaceId, _ViewName, undefined) ->
    ok;
assert_view_exists_on_provider(SpaceId, ViewName, ProviderId) ->
    case index:exists_on_provider(SpaceId, ViewName, ProviderId) of
        true ->
            ok;
        false ->
            throw(?ERROR_VIEW_NOT_EXISTS_ON(ProviderId))
    end.
