%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module provides helper methods for processing requests that were
%%%      rerouted to other provider.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_remote).
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([postrouting/3, prerouting/3]).

%%%===================================================================
%%% API functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc This function is called for each request that should be rerouted to remote provider and allows to choose
%%      the provider ({ok, {reroute, ProviderId}}), stop rerouting while giving response to the request ({ok, {response, Response}})
%%      or stop rerouting due to error.
%% @end
%%--------------------------------------------------------------------
-spec prerouting(SpaceInfo :: #space_info{}, Request :: term(), [ProviderId :: binary()]) ->
    {ok, {response, Response :: term()}} | {ok, {reroute, ProviderId :: binary(), NewRequest :: term()}} | {error, Reason :: any()}.
prerouting(_, _, []) ->
    {error, no_providers};
prerouting(_SpaceInfo, RequestBody, [RerouteTo | _Providers]) ->
    Path = <<>>,

    %% Replace all paths in this request with their "full" versions (with /spaces prefix).
    {ok, FullPath} = {ok, undefined}, %% @todo: get non-ambiguous path or file UUID
    TupleList = lists:map(
        fun(Elem) ->
            case Elem of
                Path -> FullPath;
                _    -> Elem
            end
        end, tuple_to_list(RequestBody)),
    RequestBody1 = list_to_tuple(TupleList),
    {ok, {reroute, RerouteTo, RequestBody1}}.



%%--------------------------------------------------------------------
%% @doc This function is called for each response from remote provider and allows altering this response
%%      (i.e. show empty directory instead of error in some cases).
%%      'undefined' return value means, that response is invalid and the whole rerouting process shall fail.
%% @end
%%--------------------------------------------------------------------
-spec postrouting(SpaceInfo :: #space_info{}, {ok | error, ResponseOrReason :: term()}, Request :: term()) -> Result :: undefined | term().
postrouting(#space_info{} = _SpaceInfo, {ok, Response}, _Request) ->
    Response;
postrouting(_SpaceInfo, UnkResult, Request) ->
    ?error("Unknown result ~p for request ~p", [UnkResult, Request]),
    undefined.
