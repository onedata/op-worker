%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Interface between provider and groups.
%%% Operations may involve interactions with OZ api
%%% or cached records from the datastore.
%%% @end
%%%-------------------------------------------------------------------
-module(group_logic).
-author("Michal Zmuda").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

-export([get/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves group document.
%% Provided client should be authorised to access group details.
%% @end
%%--------------------------------------------------------------------
-spec get(oz_endpoint:client(), GroupId :: binary()) ->
    {ok, datastore:document()} | {error, Reason :: term()}.
get({user, {Macaroon, DischMacaroon}}, GroupId) ->
    onedata_group:get_or_fetch(
        #auth{macaroon = Macaroon, disch_macaroons = DischMacaroon}, GroupId).




