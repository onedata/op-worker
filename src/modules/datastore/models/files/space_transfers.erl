%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing information about space transfers stats.
%%% @end
%%%-------------------------------------------------------------------
-module(space_transfers).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get/1, get/2, update/3, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type space_transfers() :: #space_transfers{}.
-type doc() :: datastore_doc:doc(space_transfers()).

-export_type([space_transfers/0, doc/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined()
%%    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfers for given space.
%% @end
%%-------------------------------------------------------------------
-spec get(SpaceId :: od_space:id()) -> space_transfers() | {error, term()}.
get(SpaceId) ->
    ?MODULE:get(oneprovider:get_id_or_undefined(), SpaceId).


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfers for given provider and space.
%% @end
%%-------------------------------------------------------------------
-spec get(ProviderId :: od_provider:id(), SpaceId :: od_space:id()) ->
    space_transfers() | {error, term()}.
get(ProviderId, SpaceId) ->
    case datastore_model:get(?CTX, datastore_utils:gen_key(ProviderId, SpaceId)) of
        {ok, Doc} -> {ok, Doc#document.value};
        Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Updates space transfers document or creates it if one doesn't exists already.
%% @end
%%--------------------------------------------------------------------
-spec update(SpaceId :: od_space:id(), SourceProviderId :: od_provider:id(),
    Bytes :: non_neg_integer()) -> ok | {error, term()}.
update(SpaceId, SourceProviderId, Bytes) ->
    ProviderId = oneprovider:get_id_or_undefined(),
    Key = datastore_utils:gen_key(ProviderId, SpaceId),
    Diff = fun(SpaceTransfers = #space_transfers{
        last_update = LastUpdateMap,
        min_hist = MinHistograms,
        hr_hist = HrHistograms,
        dy_hist = DyHistograms,
        mth_hist = MthHistograms
    }) ->
        LastUpdate = maps:get(SourceProviderId, LastUpdateMap, 0),
        CurrentTime = provider_logic:zone_time_seconds(),
        {ok, SpaceTransfers#transfer{
            last_update = maps:put(SourceProviderId, CurrentTime, LastUpdateMap),
            min_hist = transfer_histogram:update(
                SourceProviderId, Bytes, MinHistograms,
                ?FIVE_SEC_TIME_WINDOW, LastUpdate, CurrentTime
            ),
            hr_hist = transfer_histogram:update(
                SourceProviderId, Bytes, HrHistograms,
                ?MIN_TIME_WINDOW, LastUpdate, CurrentTime
            ),
            dy_hist = transfer_histogram:update(
                SourceProviderId, Bytes, DyHistograms,
                ?HOUR_TIME_WINDOW, LastUpdate, CurrentTime
            ),
            mth_hist = transfer_histogram:update(
                SourceProviderId, Bytes, MthHistograms,
                ?DAY_TIME_WINDOW, LastUpdate, CurrentTime
            )
        }}
    end,
    Default = #document{
        key = datastore_utils:gen_key(ProviderId, SpaceId),
        value = #space_transfers{
            last_update = #{},
            min_hist = #{},
            hr_hist = #{},
            dy_hist = #{},
            mth_hist = #{}
        }
    },
    case datastore_model:update(?CTX, Key, Diff, Default) of
        {ok, _} -> ok;
        Error -> Error
    end.


%%-------------------------------------------------------------------
%% @doc
%% Deletes space transfer document.
%% @end
%%-------------------------------------------------------------------
-spec delete(SpaceId :: od_space:id()) -> ok | {error, term()}.
delete(SpaceId) ->
    Key = datastore_utils:gen_key(oneprovider:get_id_or_undefined(), SpaceId),
    datastore_model:delete(?CTX, Key).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]}.
