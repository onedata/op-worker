%%%-------------------------------------------------------------------
%%% @author cyfronet
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
-module(index).
-author("cyfronet").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    delete/2, query_view_and_filter_values/3, get_json/2
    , list/4, save/6, is_supported/3]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type index() :: #index{}.
-type options() :: #{binary() => term()}.
-type providers() :: all | [od_provider:id()].
-type doc() :: datastore_doc:doc(index()).

-export_type([index/0, doc/0, options/0, providers/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    mutator => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec save(od_space:id(), name(), index_function(), options(), boolean(),
    [od_provider:id()]) -> ok.
save(SpaceId, Name, MapFunction, Options, Spatial, Providers) ->
    ok.

-spec is_supported(od_space:id(), binary(), od_provider:id()) -> boolean().
is_supported(SpaceId, IndexName, ProviderId) ->
    true.

-spec query_view_and_filter_values(od_space:id(), name(), list()) ->
    {ok, [file_meta:uuid()]}.
query_view_and_filter_values(SpaceId, IndexName, Options) ->
    {ok, []}.

-spec list(od_space:id(), undefined | name(), integer(), non_neg_integer() | all) ->
    [index:name()].
list(SpaceId, StartId, Offset, Limit) ->
    {ok, []}.

-spec get_json(od_space:id(), binary()) ->
    #{binary() => term()} | {error, term()}.
get_json(SpaceId, IndexName) ->
    #{}.

-spec delete(od_space:id(), binary()) -> ok | {error, term()}.
delete(SpaceId, IndexName) ->
    ok.

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
        {name, string},
        {spatial, atom},
        {map_function, string},
        {reduce_function, string},
        {index_options, #{string => term}},
        {providers, []} % TODO how to specify providers :: all | [od_provider:id()] ?
    ]}.
