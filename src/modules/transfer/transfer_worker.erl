%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Implementation of worker for transfer_workers_pool.
%%% Worker is responsible for replication of one file.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_worker).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-define(FILE_TRANSFER_RETRIES,
    application:get_env(?APP_NAME, max_file_transfer_retries_per_file, 5)).

-record(state, {}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([]) ->
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}).
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}).
handle_cast({start_file_replication, UserCtx, FileCtx, Block, TransferId}, State) ->
    case replicate_file(UserCtx, FileCtx, Block, TransferId, ?FILE_TRANSFER_RETRIES) of
        ok ->
            {noreply, State, hibernate};
        {error, transfer_cancelled} ->
            {noreply, State, hibernate};
        {error, _Reason} ->
            {ok, _} = transfer:increase_failed_file_transfers(TransferId),
            {noreply, State, hibernate}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}).
handle_info(_Info, State) ->
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
    State :: state()) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) ->
    {ok, NewState :: state()} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Replicates files
%% @end
%%-------------------------------------------------------------------
-spec replicate_file(user:ctx(), file_ctx:ctx(), fslogic_blocks:block(),
    transfer:id(), non_neg_integer()) -> ok | {error, term()}.
replicate_file(UserCtx, FileCtx, Block, TransferId, Retries) ->
    try
        #provider_response{status = #status{code = ?OK}} =
            sync_req:replicate_file(UserCtx, FileCtx, Block, TransferId),
        ok
    catch
        throw:{transfer_cancelled, TransferId} ->
            {error, transfer_cancelled};
        _:Reason ->
            maybe_retry(UserCtx, FileCtx, Block, TransferId, Retries, Reason)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether file replication can be retried and repeats
%% replication if it's possible.
%% @end
%%-------------------------------------------------------------------
-spec maybe_retry(user:ctx(), file_ctx:ctx(), fslogic_blocks:block(),
    transfer:id(), non_neg_integer(), term()) -> ok | {error, term()}.
maybe_retry(_UserCtx, FileCtx, _Block, TransferId, 0, Reason) ->
    {Path, _FileCtx2} = file_ctx:get_canonical_path(FileCtx),
    ?error_stacktrace(
        "Replication of file ~p in scope of transfer ~p failed due to ~p~n"
        "No retries left", [Path, TransferId, Reason]),
    {error, retries_per_file_transfer_exceeded};
maybe_retry(UserCtx, FileCtx, Block, TransferId, Retries, Reason) ->
    {Path, FileCtx2} = file_ctx:get_canonical_path(FileCtx),
    ?warning(
        "Replication of file ~p in scope of transfer ~p failed due to ~p~n"
        "File transfer will be retried (attempts left: ~p)", [Path, TransferId, Reason, Retries - 1]),
    replicate_file(UserCtx, FileCtx2, Block, TransferId, Retries - 1).