%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module allows for management of RTransfer file blocks.
%% @end
%% ===================================================================
-module(rt_heap).

-include("oneprovider_modules/rtransfer/rt_heap.hrl").

%% API
-export([new/0, new/2, delete/1, push/2, fetch/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================

%% new/0
%% ====================================================================
%% @equiv new(".", ?RT_BLOCK_SIZE)
%% @end
-spec new() -> {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
%% ====================================================================
new() ->
    new(".", ?RT_BLOCK_SIZE).


%% new/2
%% ====================================================================
%% @doc Creates RTransfer heap.
%% @end
-spec new(Prefix :: string(), BlockSize :: integer()) ->
    {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
%% ====================================================================
new(Prefix, BlockSize) ->
    gen_server:start_link(?MODULE, [Prefix, BlockSize], []).


%% push/2
%% ====================================================================
%% @doc Pushes block on RTransfer heap.
%% @end
-spec push(Pid :: pid(), Block :: #rt_block{}) -> ok.
%% ====================================================================
push(Pid, Block) ->
    gen_server:cast(Pid, {push, Block}).


%% fetch/1
%% ====================================================================
%% @doc Fetches block from RTransfer heap.
%% @end
-spec fetch(Pid :: pid()) -> {ok, #rt_block{}} | {error, Error :: string()}.
%% ====================================================================
fetch(Pid) ->
    gen_server:call(Pid, fetch).


%% delete/1
%% ====================================================================
%% @doc Deletes RTransfer heap.
%% @end
-spec delete(Pid :: pid()) -> ok.
%% ====================================================================
delete(Pid) ->
    gen_server:cast(Pid, delete).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>
%% @end
-spec init(Args :: term()) -> Result when
    Result :: {ok, State}
    | {ok, State, Timeout}
    | {ok, State, hibernate}
    | {stop, Reason :: term()}
    | ignore,
    State :: term(),
    Timeout :: non_neg_integer() | infinity.
%% ====================================================================
init([Prefix, BlockSize]) ->
    try
        erlang:load_nif(filename:join(Prefix, "c_lib/rt_heap_drv"), 0),
        {ok, Heap} = init_nif(BlockSize),
        {ok, #state{heap = Heap}}
    catch
        _:Reason -> {stop, Reason}
    end.


%% handle_call/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>
%% @end
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
    Result :: {reply, Reply, NewState}
    | {reply, Reply, NewState, Timeout}
    | {reply, Reply, NewState, hibernate}
    | {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason, Reply, NewState}
    | {stop, Reason, NewState},
    Reply :: term(),
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity,
    Reason :: term().
%% ====================================================================
handle_call(fetch, _From, #state{heap = Heap} = State) ->
    {reply, fetch_nif(Heap), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.


%% handle_cast/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>
%% @end
-spec handle_cast(Request :: term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_cast({push, Block}, #state{heap = Heap} = State) ->
    push_nif(Heap, Block),
    {noreply, State};
handle_cast(delete, State) ->
    {stop, normal, State};
handle_cast(_Request, State) ->
    {noreply, State}.


%% handle_info/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>
%% @end
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_info(_Info, State) ->
    {noreply, State}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
%% @end
-spec terminate(Reason, State :: term()) -> Any :: term() when
    Reason :: normal
    | shutdown
    | {shutdown, term()}
    | term().
%% ====================================================================
terminate(_Reason, _State) ->
    ok.


%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>
%% @end
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
    Result :: {ok, NewState :: term()} | {error, Reason :: term()},
    OldVsn :: Vsn | {down, Vsn},
    Vsn :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% NIF functions
%% ====================================================================

%% init_nif/0
%% ====================================================================
%% @doc Initializes RTransfer heap using NIF library.
%% @end
-spec init_nif(BlockSize :: integer()) -> {ok, Heap :: term()} | no_return().
%% ====================================================================
init_nif(_BlockSize) ->
    throw("NIF library not loaded.").


%% push_nif/2
%% ====================================================================
%% @doc Pushes block on RTransfer heap using NIF library.
%% @end
-spec push_nif(Heap :: term(), Block :: #rt_block{}) -> ok | no_return().
%% ====================================================================
push_nif(_Heap, _Block) ->
    throw("NIF library not loaded.").


%% fetch_nif/1
%% ====================================================================
%% @doc Fetches block from RTransfer heap using NIF library.
%% @end
-spec fetch_nif(Heap :: term()) -> {ok, #rt_block{}} | {error, Error :: string()} | no_return().
%% ====================================================================
fetch_nif(_Heap) ->
    throw("NIF library not loaded.").
