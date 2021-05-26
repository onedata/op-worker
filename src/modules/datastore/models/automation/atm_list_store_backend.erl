%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module with the persistent list store backend implemented using infinite log.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_list_store_backend).
-author("Michal Stanisz").

%% API
-export([create/1, append/2, destroy/1, list/2]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type id() :: datastore_infinite_log:key().

-export_type([id/0]).


-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API
%%%===================================================================

-spec create(infinite_log:log_opts()) -> {ok, id()}.
create(Opts) ->
    Id = datastore_key:new(),
    ok = datastore_infinite_log:create(?CTX, Id, Opts),
    {ok, Id}.


-spec append(id(), infinite_log:content()) -> ok.
append(Id, Content) ->
    datastore_infinite_log:append(?CTX, Id, Content).


-spec destroy(id()) -> ok | {error, term()}.
destroy(Id) ->
    datastore_infinite_log:destroy(?CTX, Id).


-spec list(id(), infinite_log_browser:listing_opts()) -> 
    {ok, infinite_log_browser:listing_result()} | {error, term()}.
list(Id, Opts) ->
    datastore_infinite_log:list(?CTX, Id, Opts).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.
