%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Manages data transfers
%%% @end
%%%--------------------------------------------------------------------
-module(transfer).
-author("Tomasz Lichon").

-behaviour(gen_server).

-include_lib("ctool/include/logging.hrl").

%% API
-export([start/4, get_status/1, get/1, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-type id() :: binary().
-type status() :: scheduled | active | completed | cancelled | failed.

-define(SERVER, ?MODULE).

-record(state, {session_id :: session:id()}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start(session:id(), file_meta:entry(), oneprovider:id(), binary()) ->
    {ok, id()} | ignore | {error, Reason :: term()}.
start(SessionId, FileEntry, ProviderId, Callback) ->
    {ok, Pid} = gen_server:start_link({local, ?SERVER}, ?MODULE,
        [SessionId, FileEntry, ProviderId, Callback], []),
    TransferId = pid_to_id(Pid),
    session:add_transfer(SessionId, TransferId),
    {ok, TransferId}.

%%--------------------------------------------------------------------
%% @doc
%% Gets status of the transfer
%% @end
%%--------------------------------------------------------------------
-spec get_status(TransferId :: id()) -> status().
get_status(TransferId)  ->
    gen_server:call(get_status, id_to_pid(TransferId)).

%%--------------------------------------------------------------------
%% @doc
%% Gets transfer info
%% @end
%%--------------------------------------------------------------------
-spec get(TransferId :: id()) -> list().
get(TransferId)  ->
    gen_server:call(get_info, id_to_pid(TransferId)).

%%--------------------------------------------------------------------
%% @doc
%% Stop transfer
%% @end
%%--------------------------------------------------------------------
-spec stop(id()) -> ok.
stop(TransferId) ->
    gen_server:stop(id_to_pid(TransferId)).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([SessionId, FileEntry, ProviderId, Callback]) ->
    spawn(fun() ->
        try
            ok = onedata_file_api:replicate_file(SessionId, FileEntry, ProviderId)
        catch
            _:E ->
                ?error_stacktrace("Could not replicate file ~p due to ~p", [FileEntry, E])
        end
    end),
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
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
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term().
terminate(_Reason, #state{session_id = SessionId}) ->
    session:remove_transfer(SessionId, pid_to_id(self())).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) -> {ok, NewState :: #state{}} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Converts pid of transfer handler to transfer id
%% @end
%%--------------------------------------------------------------------
-spec pid_to_id(Pid :: pid()) -> id().
pid_to_id(Pid) ->
    base64url:encode(term_to_binary(Pid)).

%%--------------------------------------------------------------------
%% @doc
%% Converts transfer id to pid of transfer handler
%% @end
%%--------------------------------------------------------------------
-spec id_to_pid(TransferId :: id()) -> pid().
id_to_pid(TransferId) ->
    binary_to_term(base64url:decode(TransferId)).