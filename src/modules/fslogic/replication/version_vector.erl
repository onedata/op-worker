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

-include("modules/dbsync/common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([compare/2, reconcile/3, bump_version/1]).

-type replica_id() :: {oneprovider:id(), file_location:id()}.
-type version_vector() :: #{}.
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
%% Updates version_vector with information about given replica. Which
%% effectively results in setting version under key of this replica to newest
%% version
%% @end
%%--------------------------------------------------------------------
-spec reconcile(version_vector(), version_vector(), replica_id()) -> version_vector().
reconcile(BaseVV, ExternalVV, ReplicaId) ->
    ReplicaVersion = max(get_version(ReplicaId, BaseVV), get_version(ReplicaId, ExternalVV)),
    maps:update(ReplicaId, ReplicaVersion, BaseVV).

%%--------------------------------------------------------------------
%% @doc
%% Bumps version of given location. This function is used whenever write event updates location.
%% @end
%%--------------------------------------------------------------------
-spec bump_version(file_location:doc()) -> term().
bump_version(Doc = #document{key = LocationId, value = Location = #file_location{provider_id = ProviderId, version_vector = VV}}) ->
    NewVersion = maps:put({ProviderId, LocationId}, get_version({ProviderId, LocationId}, VV) + 1, VV),
    Doc#document{value = Location#file_location{version_vector = NewVersion}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Get version corresponding to given replica id, from version_vector.
%% @end
%%--------------------------------------------------------------------
-spec get_version(replica_id(), version_vector()) -> neg_integer().
get_version(ReplicaId, VV) ->
    maps:get(ReplicaId, VV, 0).

%%--------------------------------------------------------------------
%% @doc
%% Return list of replica_ids which are stored in given version_vector.
%% @end
%%--------------------------------------------------------------------
-spec identificators(version_vector()) -> [replica_id()].
identificators(VVs) ->
    [Key || VV <- VVs, Key <- maps:keys(VV)].
