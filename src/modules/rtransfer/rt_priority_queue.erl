%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module allows for management of RTransfer priority queue.
%%% @end
%%%-------------------------------------------------------------------
-module(rt_priority_queue).
-author("Krzysztof Trzepla").
-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules/rtransfer/rt_container.hrl").

-on_load(load_nif/0).

%% API
-export([new/1, new/2, delete/1]).
-export([push/2, pop/1, pop/2, change_counter/4, change_counter/5, size/1]).
-export([subscribe/3, unsubscribe/2]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

%% gen_server state
%% * container - pointer to container resource created as a call to rt_container:init_nif() function
%% * size - amount of elements stored in the container
%% * subscribers - list of pairs {reference(), pid()} used to notify processes about container state
-record(state, {container_ptr, size = 0, subscribers = []}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates RTransfer priority queue with default maximal RTransfer
%% block size.
%% @end
%%--------------------------------------------------------------------
-spec new(BlockSize :: non_neg_integer()) ->
    {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
new(BlockSize) ->
    gen_server:start_link(?MODULE, [BlockSize], []).


%%--------------------------------------------------------------------
%% @doc
%% Creates RTransfer priority queue and registeres it under given
%% name.
%% @end
%%--------------------------------------------------------------------
-spec new(ContainerName :: container_name(), BlockSize :: integer()) ->
    {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
new(ContainerName, BlockSize) ->
    gen_server:start_link(ContainerName, ?MODULE, [BlockSize], []).


%%--------------------------------------------------------------------
%% @doc
%% Deletes RTransfer priority queue.
%% @end
%%--------------------------------------------------------------------
-spec delete(ContainerRef) -> ok | {error, Reason :: term()} when
    ContainerRef :: container_ref().
delete(ContainerRef) ->
    gen_server:call(ContainerRef, delete).


%%--------------------------------------------------------------------
%% @doc
%% Pushes block on RTransfer priority queue.
%% @end
%%--------------------------------------------------------------------
-spec push(ContainerRef, Block :: #rt_block{}) -> ok when
    ContainerRef :: container_ref().
push(ContainerRef, Block) ->
    gen_server:cast(ContainerRef, {push, Block}).


%%--------------------------------------------------------------------
%% @doc
%% Pops block from RTransfer priority queue.
%% @end
%%--------------------------------------------------------------------
-spec pop(ContainerRef) -> {ok, #rt_block{}} | {error, Error :: term()} when
    ContainerRef :: container_ref().
pop(ContainerRef) ->
    gen_server:call(ContainerRef, pop).


%%--------------------------------------------------------------------
%% @doc
%% Pops block from RTransfer priority queue and allows to
%% filter terms.
%% @end
%%--------------------------------------------------------------------
-spec pop(ContainerRef, TermsFilterFunction) -> {ok, #rt_block{}} | {error, Error :: term()} when
    ContainerRef :: container_ref(),
    TermsFilterFunction :: function(). %% fun(Term) -> true | false
pop(ContainerRef, TermsFilterFunction) ->
    case gen_server:call(ContainerRef, pop) of
        {ok, #rt_block{terms = Terms} = Block} ->
            {ok, Block#rt_block{
                terms = lists:filter(fun(Term) ->
                    TermsFilterFunction(Term)
                end, Terms)
            }};
        Other ->
            Other
    end.


%%--------------------------------------------------------------------
%% @equiv change_counter(ContainerRef, Offset, Size, 1)
%%--------------------------------------------------------------------
-spec change_counter(ContainerRef, FileId, Offset, Size) -> ok when
    ContainerRef :: container_ref(),
    FileId :: string(),
    Offset :: non_neg_integer(),
    Size :: non_neg_integer().
change_counter(ContainerRef, FileId, Offset, Size) ->
    change_counter(ContainerRef, FileId, Offset, Size, 1).


%%--------------------------------------------------------------------
%% @doc
%% Changes counter for block in range [Offset, Offset + Size) by
%% add Change value to current blocks' counter value.
%% @end
%%--------------------------------------------------------------------
-spec change_counter(ContainerRef, FileId, Offset, Size, Change) -> ok when
    ContainerRef :: container_ref(),
    FileId :: string(),
    Offset :: non_neg_integer(),
    Size :: non_neg_integer(),
    Change :: integer().
change_counter(ContainerRef, FileId, Offset, Size, Change) ->
    gen_server:cast(ContainerRef, {change_counter, FileId, Offset, Size, Change}).


%%--------------------------------------------------------------------
%% @doc
%% Returns size of priority queue.
%% @end
%%--------------------------------------------------------------------
-spec size(ContainerRef) -> {ok, Size :: non_neg_integer()} | {error, Error :: term()} when
    ContainerRef :: container_ref().
size(ContainerRef) ->
    gen_server:call(ContainerRef, size).


%%--------------------------------------------------------------------
%% @doc
%% Subscribes process to receive notifications when priority queue
%% changes state for empty to nonempty.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(ContainerRef, Pid :: pid(), Id :: reference()) -> ok when
    ContainerRef :: container_ref().
subscribe(ContainerRef, Pid, Id) ->
    gen_server:cast(ContainerRef, {subscribe, Pid, Id}).


%%--------------------------------------------------------------------
%% @doc
%% Unsubscribes process from receiving notifications when priority
%% queue changes state for empty to nonempty.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(ContainerRef :: container_ref(), Id :: reference()) -> ok.
unsubscribe(ContainerRef, Id) ->
    gen_server:cast(ContainerRef, {unsubscribe, Id}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% <a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State}
    | {ok, State, Timeout}
    | {ok, State, hibernate}
    | {stop, Reason :: term()}
    | ignore,
    State :: term(),
    Timeout :: non_neg_integer() | infinity.
init([BlockSize]) ->
    try
        {ok, ContainerPtr} = init_nif(BlockSize),
        {ok, #state{container_ptr = ContainerPtr}}
    catch
        _:Reason -> {stop, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>
%% @end
%%--------------------------------------------------------------------
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
handle_call(pop, _From, #state{size = 0} = State) ->
    {reply, {error, empty}, State};

handle_call(pop, _From, #state{container_ptr = ContainerPtr} = State) ->
    case pop_nif(ContainerPtr) of
        {ok, Size, Block} -> {reply, {ok, Block}, State#state{size = Size}};
        Other -> {reply, Other, State}
    end;

handle_call(size, _From, #state{size = Size} = State) ->
    {reply, {ok, Size}, State};

handle_call(delete, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


%%--------------------------------------------------------------------
%% @doc
%% <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
handle_cast({push, Block}, #state{size = Size, container_ptr = ContainerPtr, subscribers = Subscribers} = State) ->
    {ok, NewSize} = push_nif(ContainerPtr, Block),
    case Size of
        0 -> lists:foreach(fun({Id, Pid}) ->
            Pid ! {not_empty, Id}
        end, Subscribers);
        _ -> ok
    end,
    {noreply, State#state{size = NewSize}};

handle_cast({change_counter, FileId, Offset, Size, Change}, #state{container_ptr = ContainerPtr} = State) ->
    case change_counter_nif(ContainerPtr, FileId, Offset, Size, Change) of
        {ok, Size} -> {noreply, State#state{size = Size}};
        _ -> {noreply, State}
    end;

handle_cast({subscribe, Pid, Id}, #state{subscribers = Subscribers} = State) ->
    {noreply, State#state{subscribers = [{Id, Pid} | Subscribers]}};

handle_cast({unsubscribe, Id}, #state{subscribers = Subscribers} = State) ->
    {noreply, State#state{subscribers = proplists:delete(Id, Subscribers)}};

handle_cast(_Request, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% @doc
%% <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
handle_info(_Info, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% @doc
%% <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason, State :: term()) -> Any :: term() when
    Reason :: normal
    | shutdown
    | {shutdown, term()}
    | term().
terminate(_Reason, _State) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% <a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
    Result :: {ok, NewState :: term()} | {error, Reason :: term()},
    OldVsn :: Vsn | {down, Vsn},
    Vsn :: term().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% NIF functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Loads RTransfer priority queue NIF library.
%% @end
%%--------------------------------------------------------------------
-spec load_nif() -> ok | {error, Reason :: term()}.
load_nif() ->
    LibName = "rt_priority_queue_drv",
    LibPath =
        case code:priv_dir(?APP_NAME) of
            {error, bad_name} ->
                case filelib:is_dir(filename:join(["..", priv])) of
                    true ->
                        filename:join(["..", priv, LibName]);
                    _ ->
                        filename:join([priv, LibName])
                end;

            Dir ->
                filename:join(Dir, LibName)
        end,

    case erlang:load_nif(LibPath, 0) of
        ok -> ok;
        {error, {reload, _}} -> ok;
        {error, {upgrade, _}} -> ok;
        {error, Reason} -> {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Initializes RTransfer map using NIF library.
%% @end
%%--------------------------------------------------------------------
-spec init_nif(BlockSize :: integer()) ->
    {ok, ContainerPtr :: container_ptr()} | no_return().
init_nif(_BlockSize) ->
    erlang:nif_error(nif_library_not_loaded).


%%--------------------------------------------------------------------
%% @doc
%% Pushes block on RTransfer map using NIF library.
%% @end
%%--------------------------------------------------------------------
-spec push_nif(ContainerPtr :: container_ptr(), Block :: #rt_block{}) ->
    {ok, Size :: non_neg_integer()} | no_return().
push_nif(_ContainerPtr, _Block) ->
    erlang:nif_error(nif_library_not_loaded).


%%--------------------------------------------------------------------
%% @doc
%% Pops block from RTransfer map using NIF library.
%% @end
%%--------------------------------------------------------------------
-spec pop_nif(ContainerPtr :: container_ptr()) ->
    {ok, Size :: non_neg_integer(), Block :: #rt_block{}} |
    {error, Error :: term()} | no_return().
pop_nif(_ContainerPtr) ->
    erlang:nif_error(nif_library_not_loaded).


%%--------------------------------------------------------------------
%% @doc
%% Changes counter for block in range [Offset, Offset + Size) by
%% add Change value to current blocks' counter value.
%%--------------------------------------------------------------------
%% @end
-spec change_counter_nif(ContainerPtr, FileId, Offset, Size, Change) ->
    {ok, Size} | no_return() when
    ContainerPtr :: container_ptr(),
    FileId :: string(),
    Offset :: non_neg_integer(),
    Size :: non_neg_integer(),
    Change :: integer().
change_counter_nif(_ContainerPtr, _FileId, _Offset, _Size, _Change) ->
    erlang:nif_error(nif_library_not_loaded).
