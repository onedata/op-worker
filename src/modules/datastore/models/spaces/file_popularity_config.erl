%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This model stores file-popularity configuration.
%%% @end
%%%-------------------------------------------------------------------
-module(file_popularity_config).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

-type id() :: od_space:id().
-type record() :: #file_popularity_config{}.
-type diff() :: datastore:doc(record()).
-type doc() :: datastore_model:doc(record()).
-type error() :: {error, term()}.

-export_type([id/0]).

%% API
-export([disable/1, enable/1, is_enabled/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-define(CTX, #{
    model => ?MODULE,
    generated_key => false
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec enable(id()) -> ok | error().
enable(SpaceId) ->
    ok = ?extract_ok(create_or_update(SpaceId, fun(FPC) ->
        {ok, FPC#file_popularity_config{enabled = true}}
    end, default_doc(SpaceId, true))).

-spec disable(id()) -> ok | error().
disable(SpaceId) ->
    ok = ?extract_ok(create_or_update(SpaceId, fun(FPC) ->
        {ok, FPC#file_popularity_config{enabled = false}}
    end, default_doc(SpaceId, false))).

-spec is_enabled(id()) -> boolean().
is_enabled(SpaceId) ->
    case datastore_model:get(?CTX, SpaceId) of
        {ok, #document{value = FPC}} ->
            FPC#file_popularity_config.enabled;
        _ ->
            false
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec default_doc(id(), boolean()) -> doc().
default_doc(SpaceId, Enabled) ->
    #document{
        key = SpaceId,
        value = #file_popularity_config{enabled = Enabled},
        scope = SpaceId
    }.

-spec create_or_update(id(), diff(), doc()) -> {ok, doc()} | error().
create_or_update(SpaceId, UpdateFun, DefaultDoc) ->
    datastore_model:update(?CTX, SpaceId, UpdateFun, DefaultDoc).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
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
    {record, [{enabled, boolean}]}.