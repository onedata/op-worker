%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing aggregated statistics about transfers
%%% featuring given space and target provider.
%%% @end
%%%-------------------------------------------------------------------
-module(space_transfer).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get/1, get/2, update/4, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type timestamp() :: non_neg_integer().
-type size() :: pos_integer().
-type space_transfer() :: #space_transfer{}.
-type doc() :: datastore_doc:doc(space_transfer()).

-export_type([space_transfer/0, doc/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    mutator => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfers for given space.
%% @end
%%-------------------------------------------------------------------
-spec get(SpaceId :: od_space:id()) -> space_transfer() | {error, term()}.
get(SpaceId) ->
    ?MODULE:get(oneprovider:get_id_or_undefined(), SpaceId).


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfers for given provider and space.
%% @end
%%-------------------------------------------------------------------
-spec get(ProviderId :: od_provider:id(), SpaceId :: od_space:id()) ->
    space_transfer() | {error, term()}.
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
    Bytes :: size(), CurrentTime :: timestamp()) -> ok | {error, term()}.
update(SpaceId, SourceProviderId, Bytes, CurrentTime) ->
    Key = datastore_utils:gen_key(oneprovider:get_id_or_undefined(), SpaceId),
    Diff = fun(SpaceTransfers = #space_transfer{
        last_update = LastUpdateMap,
        min_hist = MinHistograms,
        hr_hist = HrHistograms,
        dy_hist = DyHistograms,
        mth_hist = MthHistograms
    }) ->
        LastUpdate = maps:get(SourceProviderId, LastUpdateMap, 0),
        {ok, SpaceTransfers#space_transfer{
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
        scope = SpaceId,
        key = Key,
        value = #space_transfer{
            last_update = #{SourceProviderId => CurrentTime},
            min_hist = transfer_histogram:update(
                SourceProviderId, Bytes, #{}, ?FIVE_SEC_TIME_WINDOW, 0, CurrentTime
            ),
            hr_hist = transfer_histogram:update(
                SourceProviderId, Bytes, #{}, ?MIN_TIME_WINDOW, 0, CurrentTime
            ),
            dy_hist = transfer_histogram:update(
                SourceProviderId, Bytes, #{}, ?HOUR_TIME_WINDOW, 0, CurrentTime
            ),
            mth_hist = transfer_histogram:update(
                SourceProviderId, Bytes, #{}, ?DAY_TIME_WINDOW, 0, CurrentTime
            )
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
