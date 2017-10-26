%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for controlling autocleaning operation.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_controller).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0, maybe_start/2, stop/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    autocleaning_id :: autocleaning:id(),
    space_id :: od_space:id(),
    file_size_gt :: undefined | non_neg_integer(),
    file_size_lt :: undefined | non_neg_integer(),
    max_inactive :: undefined | non_neg_integer(),
    target :: non_neg_integer(),
    threshold :: non_neg_integer()  % todo redundant???
}).

-define(START_CLEAN, start_autocleaning).
-define(STOP_CLEAN, finish_autocleaning).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% This function starts autocleaning if given AutocleaningId is set
%% in space_storage model as being in progress.
%% @end
%%-------------------------------------------------------------------
-spec maybe_start(autocleaning:id(), autocleaning:autocleaning()) -> ok.
maybe_start(AutocleaningId, AC = #autocleaning{space_id = SpaceId}) ->
    case space_storage:get_cleanup_in_progress(SpaceId) of
        AutocleaningId ->
            start(AutocleaningId, AC);
        _ ->
            autocleaning:remove_skipped(AutocleaningId, SpaceId)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


%%-------------------------------------------------------------------
%% @doc
%% Stops transfer_controller process and marks transfer as completed.
%% @end
%%-------------------------------------------------------------------
-spec stop() -> ok.
stop() ->
    gen_server2:cast(self(), ?STOP_CLEAN).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([AutocleaningId, SpaceId, FileSizeGt, FileSizeLt, MaxInactive, Target, Threshold]) ->
    ok = gen_server2:cast(self(), ?START_CLEAN),
    ?info("Autocleaning: ~p started in space ~p", [AutocleaningId, SpaceId]),
    {ok, _} = autocleaning:mark_active(AutocleaningId),
    {ok, #state{
        autocleaning_id = AutocleaningId,
        space_id = SpaceId,
        file_size_gt = FileSizeGt,
        file_size_lt = FileSizeLt,
        max_inactive = MaxInactive,
        target = Target,
        threshold = Threshold
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(?START_CLEAN, State = #state{
    autocleaning_id = AutocleaningId,
    space_id = SpaceId,
    file_size_lt = FileSizeLt,
    file_size_gt = FileSizeGt,
    max_inactive = MaxInactive,
    target = Target
}) ->
    try
        space_cleanup_api:cleanup_space(SpaceId, AutocleaningId, FileSizeGt, FileSizeLt, MaxInactive, Target),
        {noreply, State}
    catch
        _:Reason ->
            autocleaning:mark_failed(AutocleaningId),
            ?error_stacktrace("Could not autoclean space ~p due to ~p", [SpaceId, Reason]),
            {stop, Reason, State}
    end;
handle_cast(?STOP_CLEAN, State = #state{
    autocleaning_id = AutocleaningId,
    space_id = SpaceId
}) ->
    autocleaning:mark_completed(AutocleaningId),
    ?critical("Autocleaning ~p of space ~p finished", [AutocleaningId, SpaceId]),
    {stop, normal, State};
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
    ?log_bad_request(_Info),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Starts process responsible for performing autocleaning
%% @end
%%-------------------------------------------------------------------
-spec start(autocleaning:id(), autocleaning:autocleaning()) -> ok.
start(AutocleaningId, AC = #autocleaning{space_id = SpaceId}) ->
    #autocleaning_config{
        lower_file_size_limit = FileSizeGt,
        upper_file_size_limit = FileSizeLt,
        max_file_not_opened_hours = MaxInactive,
        target = Target,
        threshold = Threshold
    } = autocleaning:get_config(AC),
    {ok, _Pid} = gen_server2:start(autocleaning_controller, [AutocleaningId,
        SpaceId, FileSizeGt, FileSizeLt, MaxInactive, Target, Threshold
    ], []),
    ok.