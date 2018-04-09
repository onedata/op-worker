%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Version vector used to track replica changes
%%% @end
%%%--------------------------------------------------------------------
-module(version_vector).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([compare/2, merge_location_versions/2, bump_version/1, version_diff/2,
    replica_id_is_greater/2]).

-type replica_id() :: {oneprovider:id(), file_location:id()}.
-type version_vector() :: #{{binary(), binary()} => non_neg_integer()}.
-type comparsion_result() :: lesser | greater | identical | concurrent.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Compare two version vectors for its precedence.
%% @end
%%--------------------------------------------------------------------
-spec compare(VV1 :: version_vector(), VV2 :: version_vector()) -> comparsion_result().
compare(VV1, VV2) ->
    Comparison = [get_version(ReplicaId, VV1) - get_version(ReplicaId, VV2) || ReplicaId <- identificators([VV1, VV2])],
    case lists:all(fun(X) -> X =:= 0 end, Comparison) of
        true -> identical;
        false ->
            case lists:all(fun(X) -> X >= 0 end, Comparison) of
                true -> greater;
                false ->
                    case lists:all(fun(X) -> X =< 0 end, Comparison) of
                        true -> lesser;
                        false -> concurrent
                    end
                    
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Bumps version of given location. This function is used whenever write event updates location.
%% @end
%%--------------------------------------------------------------------
-spec bump_version(file_location:doc()) -> term().
bump_version(Doc = #document{value = Location = #file_location{version_vector = VV}}) ->
    ReplicaId = get_replica_id(Doc),
    NewVersion = maps:put(ReplicaId, get_version(ReplicaId, VV) + 1, VV),
    Doc#document{value = Location#file_location{version_vector = NewVersion}}.

%%--------------------------------------------------------------------
%% @doc
%% Updates Local file location version vector with new version of external file location.
%% @end
%%--------------------------------------------------------------------
-spec merge_location_versions(file_location:doc(), file_location:doc()) -> term().
merge_location_versions(LocalDoc = #document{value = LocalLocation = #file_location{version_vector = LocalVV}},
    ExternalDoc = #document{value = #file_location{version_vector = ExternalVV}}) ->
    ExternalReplicaId = get_replica_id(ExternalDoc),
    LocalDoc#document{value = LocalLocation#file_location{version_vector = merge(LocalVV, ExternalVV, ExternalReplicaId)}}.

%%--------------------------------------------------------------------
%% @doc
%% Return diff in version of remote location, and local location's stored version
%% of remote location.
%% @end
%%--------------------------------------------------------------------
-spec version_diff(Local :: file_location:doc(), Remote :: file_location:doc()) -> integer().
version_diff(#document{value = #file_location{version_vector = LocalVV}},
    ExternalDoc = #document{value = #file_location{version_vector = ExternalVV}}) ->
    ExternalReplicaId = get_replica_id(ExternalDoc),
    get_version(ExternalReplicaId, ExternalVV) - get_version(ExternalReplicaId, LocalVV).

%%--------------------------------------------------------------------
%% @doc
%% Returns true if replica_id associated with first document is grater that
%% second.
%% @end
%%--------------------------------------------------------------------
-spec replica_id_is_greater(file_location:doc(), file_location:doc()) -> boolean().
replica_id_is_greater(LocalDoc, ExternalDoc) ->
    get_replica_id(LocalDoc) > get_replica_id(ExternalDoc).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Get version corresponding to given replica id, from version_vector.
%% @end
%%--------------------------------------------------------------------
-spec get_version(replica_id(), version_vector()) -> non_neg_integer().
get_version(ReplicaId, VV) ->
    maps:get(ReplicaId, VV, 0).

%%--------------------------------------------------------------------
%% @doc
%% Return list of replica_ids which are stored in given version_vector.
%% @end
%%--------------------------------------------------------------------
-spec identificators([version_vector()]) -> [replica_id()].
identificators(VVs) ->
    [Key || VV <- VVs, Key <- maps:keys(VV)].

%%--------------------------------------------------------------------
%% @doc
%% Returns id of given replica (file_location), which is tuple with
%% {ProviderId, FileLocationId}
%% @end
%%--------------------------------------------------------------------
-spec get_replica_id(file_location:doc()) -> {oneprovider:id(), file_location:id()}.
get_replica_id(#document{key = LocationId, value = #file_location{provider_id = ProviderId}}) ->
    {ProviderId, LocationId}.

%%--------------------------------------------------------------------
%% @doc
%% Updates version_vector with information about given replica. Which
%% effectively results in setting version under key of this replica to newest
%% version
%% @end
%%--------------------------------------------------------------------
-spec merge(version_vector(), version_vector(), replica_id()) -> version_vector().
merge(LocalVV, ExternalVV, ReplicaId) ->
    ReplicaVersion = max(get_version(ReplicaId, LocalVV), get_version(ReplicaId, ExternalVV)),
    maps:put(ReplicaId, ReplicaVersion, LocalVV).