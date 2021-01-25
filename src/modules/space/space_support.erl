%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for space support related operations.
%%%
%%% Space support can be in one of the following stages: 
%%%     * joining 
%%%     * active 
%%%     * {resizing, TargetSize} 
%%%     * purging 
%%%     * retiring 
%%%     * retired 
%%% For more details consult `support_stage.erl`. 
%%% Stage {resizing, 0} means that support is being revoked and necessary 
%%% cleanup has been scheduled (see `space_unsupport_engine.erl`).
%%% @end
%%%--------------------------------------------------------------------
-module(space_support).
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([add/3, update_support_size/3, revoke/2]).
-export([supports_any_space/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec add(storage:id(), tokens:serialized(), od_space:support_size()) ->
    {ok, od_space:id()} | errors:error().
add(StorageId, SerializedToken, SupportSize) ->
    case validate_support_request(SerializedToken) of
        {ok, SpaceId} ->
            % remove possible remnants of previous support 
            % (when space was unsupported in Onezone without provider knowledge)
            ok = space_unsupport_engine:clean_local_documents(SpaceId, StorageId),
            % call using ?MODULE for mocking in tests
            case storage:init_space_support(StorageId, SerializedToken, SupportSize) of
                {ok, SpaceId} ->
                    on_init_support(SpaceId, StorageId),
                    %% @TODO VFS-7170 Run only after space became active
                    on_finalize_support(SpaceId),
                    {ok, SpaceId};
                {error, _} = Error ->
                    Error
            end;
        Error ->
            Error
    end.


-spec update_support_size(storage:id(), od_space:id(), NewSupportSize :: integer()) ->
    ok | errors:error().
update_support_size(StorageId, SpaceId, NewSupportSize) ->
    CurrentOccupiedSize = space_quota:current_size(SpaceId),
    case NewSupportSize < CurrentOccupiedSize of
        true -> ?ERROR_BAD_VALUE_TOO_LOW(<<"size">>, CurrentOccupiedSize);
        false -> storage:update_space_support_size(StorageId, SpaceId, NewSupportSize)
    end.


-spec revoke(storage:id(), od_space:id()) -> ok | errors:error().
revoke(StorageId, SpaceId) ->
    space_unsupport_engine:schedule_start(SpaceId, StorageId).


-spec supports_any_space(storage:id()) -> boolean() | errors:error().
supports_any_space(StorageId) ->
    case storage_logic:get_spaces(StorageId) of
        {ok, []} -> false;
        {ok, _Spaces} -> true;
        {error, _} = Error -> Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if given token is valid support token and whether provider
%% does not already support this space.
%% @TODO VFS-5497 This check will not be needed when multisupport is implemented
%% @end
%%--------------------------------------------------------------------
-spec validate_support_request(tokens:serialized()) -> {ok, od_space:id()} | errors:error().
validate_support_request(SerializedToken) ->
    case tokens:deserialize(SerializedToken) of
        {ok, #token{type = ?INVITE_TOKEN(?SUPPORT_SPACE, SpaceId)}} ->
            case provider_logic:supports_space(SpaceId) of
                true ->
                    ?ERROR_RELATION_ALREADY_EXISTS(
                        od_space, SpaceId, od_provider, oneprovider:get_id()
                    );
                false -> {ok, SpaceId}
            end;
        {ok, #token{type = ReceivedType}} ->
            ?ERROR_BAD_VALUE_TOKEN(<<"token">>,
                ?ERROR_NOT_AN_INVITE_TOKEN(?SUPPORT_SPACE, ReceivedType));
        {error, _} = Error ->
            ?ERROR_BAD_VALUE_TOKEN(<<"token">>, Error)
    end.


%% @private
-spec on_init_support(od_space:id(), storage:id()) -> ok.
on_init_support(SpaceId, StorageId) ->
    supported_spaces:add(SpaceId, StorageId).


%% @private
-spec on_finalize_support(od_space:id()) -> ok.
on_finalize_support(SpaceId) ->
    ok = qos_hooks:reevaluate_all_impossible_qos_in_space(SpaceId).
