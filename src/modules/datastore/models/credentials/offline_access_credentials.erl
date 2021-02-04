%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Offline access credentials management model.
%%% @end
%%%-------------------------------------------------------------------
-module(offline_access_credentials).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([save/2, get/1, update/2, remove/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).

-type id() :: binary().
-type record() :: #offline_access_credentials{}.
-type diff() :: datastore_doc:diff(record()).
-type doc() :: datastore_doc:doc(record()).

-export_type([id/0, record/0, doc/0]).

-define(CTX, #{
    model => ?MODULE
}).


%%%===================================================================
%%% API
%%%===================================================================


-spec save(id(), record()) -> ok.
save(Id, Record) ->
    {ok, _} = datastore_model:save(?CTX, #document{key = Id, value = Record}),
    ok.


-spec get(id()) -> {ok, record()} | ?ERROR_NOT_FOUND.
get(Id) ->
    case datastore_model:get(?CTX, Id) of
        {ok, Doc} -> {ok, Doc#document.value};
        {error, not_found} -> ?ERROR_NOT_FOUND
    end.


-spec update(id(), diff()) -> {ok, record()}.
update(Id, Diff) ->
    {ok, UpdatedDoc} = datastore_model:update(?CTX, Id, Diff),
    {ok, UpdatedDoc#document.value}.


-spec remove(id()) -> ok.
remove(Id) ->
    ok = datastore_model:delete(?CTX, Id).


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
    {record, [
        {user_id, string},
        {access_token, string},
        {interface, atom},
        {data_access_caveats_policy, atom},
        {valid_until, integer},
        {next_renewal_threshold, integer},
        {next_renewal_backoff, integer}
    ]}.
