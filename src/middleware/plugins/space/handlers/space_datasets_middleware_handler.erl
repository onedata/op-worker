%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to space datasets aspects.
%%% @end
%%%-------------------------------------------------------------------
-module(space_datasets_middleware_handler).
-author("Bartosz Walkowicz").

-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/privileges.hrl").

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).


-define(MAX_LIST_LIMIT, 1000).
-define(DEFAULT_LIST_LIMIT, 1000).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = get, gri = #gri{aspect = As}}) when
    As =:= datasets;
    As =:= datasets_details
-> #{
    required => #{
        <<"state">> => {atom, [?ATTACHED_DATASET, ?DETACHED_DATASET]}
    },
    optional => #{
        <<"offset">> => {integer, any},
        <<"index">> => {binary, any},
        <<"token">> => {binary, non_empty},
        <<"limit">> => {integer, {between, 1, ?MAX_LIST_LIMIT}}
    }
}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) -> {ok, middleware:versioned_entity()}.
fetch_entity(_) ->
    {ok, {undefined, 1}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = ?GUEST}, _) ->
    false;

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = As
}}, _) when
    As =:= datasets;
    As =:= datasets_details
->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = As}}, _) when
    As =:= datasets;
    As =:= datasets_details
->
    middleware_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{auth = Auth, gri = #gri{id = SpaceId, aspect = Aspect}, data = Data}, _)
    when Aspect =:= datasets
    orelse Aspect =:= datasets_details
->
    State = maps:get(<<"state">>, Data, ?ATTACHED_DATASET),
    ListingOpts = dataset_middleware_plugin:gather_listing_opts(Data),
    ListingMode = case Aspect of
        datasets -> ?BASIC_INFO;
        datasets_details -> ?EXTENDED_INFO
    end,
    {ok, {Datasets, IsLast}} = ?throw_on_error(opl_datasets:list_top_datasets(
        Auth#auth.session_id, SpaceId, State, ListingOpts, ListingMode
    )),
    {ok, value, {Datasets, IsLast}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(_) ->
    ?ERROR_NOT_SUPPORTED.
