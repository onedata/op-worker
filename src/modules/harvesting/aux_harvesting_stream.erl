%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements harvesting_stream behaviour. It is started
%%% by main_harvesting_stream when there is a pair {HarvesterId, IndexId}
%%% for which harvesting is delayed in comparison to main_harvesting_stream.
%%% After reaching the defined couchbase_changes:until() value, it tries
%%% to relay responsibility of harvesting in {HarvesterId, IndexId}
%%% to main_harvesting_stream.
%%% @end
%%%-------------------------------------------------------------------
-module(aux_harvesting_stream).
-author("Jakub Kudzia").

-behaviour(harvesting_stream).

-include("modules/harvesting/harvesting.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([reject_takeover/2, space_removed/1, space_unsupported/1]).

%% harvesting_stream callbacks
-export([init/1, name/1, handle_call/3, handle_cast/2, custom_error_handling/2,
    on_end_of_stream/1, on_harvesting_doc_not_found/1
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec reject_takeover(harvesting_stream:name(), couchbase_changes:seq()) -> ok.
reject_takeover(Name, NewUntil) ->
    gen_server2:cast({global, Name}, ?TAKEOVER_REJECTED(NewUntil)).

-spec space_removed(harvesting_stream:name()) -> ok.
space_removed(Name) ->
    gen_server2:cast({global, Name}, ?SPACE_REMOVED).

-spec space_unsupported(harvesting_stream:name()) -> ok.
space_unsupported(Name) ->
    gen_server2:cast({global, Name}, ?SPACE_UNSUPPORTED).

%%%===================================================================
%%% harvesting_stream callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link harvesting_stream} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init([term()]) -> {ok, harvesting_stream:state()}.
init([SpaceId, HarvesterId, IndexId, Until]) ->
    {ok, #hs_state{
        name = ?AUX_HARVESTING_STREAM(SpaceId, HarvesterId, IndexId),
        space_id = SpaceId,
        destination = harvesting_destination:init(HarvesterId, IndexId),
        until = Until + 1   % until is exclusive in couchbase_changes_stream
    }}.

%%--------------------------------------------------------------------
%% @doc
%% {@link harvesting_stream} callback name/1.
%% @end
%%--------------------------------------------------------------------
-spec name([term()]) -> harvesting_stream:name().
name([SpaceId, HarvesterId, IndexId | _]) ->
    ?AUX_HARVESTING_STREAM(SpaceId, HarvesterId, IndexId).

%%--------------------------------------------------------------------
%% @doc
%% {@link harvesting_stream} callback handle_call/3.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    harvesting_stream:state()) -> harvesting_stream:handling_result().
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @doc
%% {@link harvesting_stream} callback handle_cast/2.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: harvesting_stream:state()) ->
    harvesting_stream:handling_result().
handle_cast(?TAKEOVER_REJECTED(NewUntil), State) ->
    {noreply, harvesting_stream:enter_streaming_mode(
        State#hs_state{until = NewUntil + 1})
    };
handle_cast(?SPACE_REMOVED, State) ->
    {stop, normal, State};
handle_cast(?SPACE_UNSUPPORTED, State) ->
    {stop, normal, State};
handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @doc
%% {@link harvesting_stream} callback custom_error_handling/2.
%% @end
%%--------------------------------------------------------------------
-spec custom_error_handling(harversting_stream:state(), harvesting_result:result()) ->
    harvesting_stream:handling_result().
custom_error_handling(State = #hs_state{
    name = Name,
    destination = Destination
}, Result) ->
    % for aux_stream we are sure that there is only one key and one value
    case hd(maps:keys(harvesting_result:get_summary(Result))) of
        ?ERROR_NOT_FOUND ->
            % harvester was deleted, stream should be stopped
            {stop, normal, State};
        ?ERROR_FORBIDDEN ->
            % harvester was deleted from space, stream should be stopped
            {stop, normal, State};
        ?ERROR_TEMPORARY_FAILURE ->
            [HarvesterId] = harvesting_destination:get_harvesters(Destination),
            ErrorLog =  str_utils:format_bin("Harvester ~p is temporarily unavailable.", [HarvesterId]),
            {noreply, harvesting_stream:enter_retrying_mode(State#hs_state{
                error_log = ErrorLog,
                log_level = warning
            })};
        Error = {error, _} ->
            [HarvesterId] = harvesting_destination:get_harvesters(Destination),
            ErrorLog =  str_utils:format_bin(
                "Unexpected error ~w occurred for harvester ~p", [Error, HarvesterId]
            ),
            {noreply, harvesting_stream:enter_retrying_mode(State#hs_state{
                error_log = ErrorLog,
                log_level = error
            })};
        LastSuccessfulSeq ->
            % there might be only one index here
            case harvesting_state:set_seen_seq(Name, Destination, LastSuccessfulSeq) of
                ok ->
                    ErrorLog =  str_utils:format_bin(
                        "Unexpected error occurred when applying batch of changes."
                        "Last successful sequence number was: ~p",
                        [LastSuccessfulSeq]
                    ),
                    {noreply, harvesting_stream:enter_retrying_mode(State#hs_state{
                        error_log = ErrorLog,
                        log_level = error,
                        last_persisted_seq = LastSuccessfulSeq
                    })};
                ?ERROR_NOT_FOUND ->
                    harvesting_stream:throw_harvesting_not_found_exception(State)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link harvesting_stream} callback on_end_of_stream/1.
%% @end
%%--------------------------------------------------------------------
-spec on_end_of_stream(harvesting_stream:state()) -> ok.
on_end_of_stream(#hs_state{name = Name, until = Until}) ->
    main_harvesting_stream:propose_takeover(Name, Until - 1).

%%--------------------------------------------------------------------
%% @doc
%% {@link harvesting_stream} callback on_harvesting_doc_not_found/1.
%% @end
%%--------------------------------------------------------------------
-spec on_harvesting_doc_not_found(harvesting_stream:state()) ->
    {stop, normal, harvesting_stream:state()}.
on_harvesting_doc_not_found(State) ->
    {stop, normal, State}.