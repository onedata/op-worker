%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module allows for management of RTransfer container.
%% @end
%% ===================================================================
-module(rt_container).

-include("registered_names.hrl").
-include("oneprovider_modules/rtransfer/rt_container.hrl").

%% API
-export([new/0, new/1, new/2, new/3, new/4, delete/1]).
-export([push/2, fetch/1, fetch/2, fetch/3, size/1]).

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
%% @doc Creates RTransfer container with default prefix and maximal
%% RTransfer block size and of type priority queue.
%% @end
-spec new() -> {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
%% ====================================================================
new() ->
    new(priority_queue).


%% new/1
%% ====================================================================
%% @doc Creates RTransfer container with default prefix and maximal
%% RTransfer block size.
%% @end
-spec new(Type :: priority_queue | map) ->
    {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
%% ====================================================================
new(Type) ->
    {ok, BlockSize} = application:get_env(?APP_Name, max_rt_block_size),
    new(Type, ".", BlockSize).


%% new/2
%% ====================================================================
%% @doc Same as new/0, but allows to register container under given name.
%% @end
-spec new(Type, ContainerName) -> {ok, Pid :: pid()} | ignore | {error, Error :: term()} when
    Type :: priority_queue | map,
    ContainerName :: container_name().
%% ====================================================================
new(Type, ContainerName) ->
    {ok, BlockSize} = application:get_env(?APP_Name, max_rt_block_size),
    new(Type, ContainerName, ".", BlockSize).


%% new/3
%% ====================================================================
%% @doc Creates RTransfer container.
%% @end
-spec new(Type :: priority_queue | map, Prefix :: string(), BlockSize :: integer()) ->
    {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
%% ====================================================================
new(Type, Prefix, BlockSize) ->
    gen_server:start_link(?MODULE, [Type, Prefix, BlockSize], []).


%% new/4
%% ====================================================================
%% @doc Creates RTransfer container and registeres it under given name.
%% @end
-spec new(Type, ContainerName, Prefix :: string(), BlockSize :: integer()) ->
    {ok, Pid :: pid()} | ignore | {error, Error :: term()} when
    Type :: priority_queue | map,
    ContainerName :: container_name().
%% ====================================================================
new(Type, ContainerName, Prefix, BlockSize) ->
    gen_server:start_link(ContainerName, ?MODULE, [Type, Prefix, BlockSize], []).


%% push/2
%% ====================================================================
%% @doc Pushes block on RTransfer container.
%% @end
-spec push(ContainerRef, Block :: #rt_block{}) -> ok when
    ContainerRef :: container_ref().
%% ====================================================================
push(ContainerRef, #rt_block{provider_id = ProviderId} = Block) when is_binary(ProviderId) ->
    push(ContainerRef, Block#rt_block{provider_id = binary_to_list(ProviderId)});

push(ContainerRef, #rt_block{provider_id = ProviderId} = Block) when is_list(ProviderId) ->
    gen_server:cast(ContainerRef, {push, Block}).


%% fetch/1
%% ====================================================================
%% @doc Fetches block from RTransfer container.
%% @end
-spec fetch(ContainerRef) -> {ok, #rt_block{}} | {error, Error :: string()} when
    ContainerRef :: container_ref().
%% ====================================================================
fetch(ContainerRef) ->
    fetch(ContainerRef, fun erlang:is_process_alive/1).


%% fetch/2
%% ====================================================================
%% @doc Fetches block from RTransfer container and allows to fillter pids
%% @end
-spec fetch(ContainerRef, PidsFilterFunction) -> {ok, #rt_block{}} | {error, Error :: string()} when
    ContainerRef :: container_ref(),
    PidsFilterFunction :: function(). %% fun(Pid) -> true | false;
%% ====================================================================
fetch(ContainerRef, PidsFilterFunction) ->
    case gen_server:call(ContainerRef, fetch) of
        {ok, #rt_block{pids = Pids, provider_id = ProviderId} = Block} ->
            {ok, Block#rt_block{
                pids = lists:filter(fun(Pid) ->
                    PidsFilterFunction(Pid)
                end, lists:usort(Pids)),
                provider_id = list_to_binary(ProviderId)
            }};
        Other ->
            Other
    end.


%% fetch/3
%% ====================================================================
%% @doc Fetches blocks from RTransfer container that matches segment
%% given as offset and size using NIF library.
%% @end
-spec fetch(ContainerRef, Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    {ok, [#rt_block{}]} | {error, Error :: string()} | no_return() when
    ContainerRef :: container_ref().
%% ====================================================================
fetch(ContainerRef, Offset, Size) ->
    gen_server:call(ContainerRef, {fetch, Offset, Size}).


%% size/1
%% ====================================================================
%% @doc Returns size of container.
%% @end
-spec size(ContainerRef) ->
    {ok, [#rt_block{}]} | {error, Error :: string()} | no_return() when
    ContainerRef :: container_ref().
%% ====================================================================
size(ContainerRef) ->
    gen_server:call(ContainerRef, size).


%% delete/1
%% ====================================================================
%% @doc Deletes RTransfer container.
%% @end
-spec delete(ContainerRef) -> ok | {error, Reason :: term()} when
    ContainerRef :: container_ref().
%% ====================================================================
delete(ContainerRef) ->
    gen_server:call(ContainerRef, delete).


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
init([Type, Prefix, BlockSize]) ->
    try
        erlang:load_nif(filename:join(Prefix, "c_lib/rt_container_drv"), 0),
        {ok, Container} = init_nif(Type, BlockSize),
        {ok, #state{container = Container, type = Type}}
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
handle_call(fetch, _From, #state{size = 0} = State) ->
    {reply, {error, "Empty container"}, State};

handle_call(fetch, _From, #state{container = Container} = State) ->
    case fetch_nif(Container) of
        {ok, Size, Block} -> {reply, {ok, Block}, State#state{size = Size}};
        Other -> Other
    end;

handle_call({fetch, _Offset, _Size}, _From, #state{size = 0} = State) ->
    {reply, {error, "Empty container"}, State};

handle_call({fetch, Offset, Size}, _From, #state{container = Container} = State) ->
    case fetch_nif(Container, Offset, Size) of
        {ok, Size, Blocks} -> {reply, {ok, Blocks}, State#state{size = Size}};
        Other -> Other
    end;

handle_call(size, _From, #state{size = Size} = State) ->
    {reply, {ok, Size}, State};

handle_call(delete, _From, State) ->
    {stop, normal, ok, State};

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
handle_cast({push, Block}, #state{container = Container} = State) ->
    case push_nif(Container, Block) of
        {ok, Size} -> {noreply, State#state{size = Size}};
        Other -> Other
    end;

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

%% init_nif/1
%% ====================================================================
%% @doc Initializes RTransfer container using NIF library.
%% @end
-spec init_nif(Type :: priority_queue | map, BlockSize :: integer()) ->
    {ok, ContainerPtr :: container_ptr()} | no_return().
%% ====================================================================
init_nif(_Type, _BlockSize) ->
    throw("NIF library not loaded.").


%% push_nif/2
%% ====================================================================
%% @doc Pushes block on RTransfer container using NIF library.
%% @end
-spec push_nif(ContainerPtr :: container_ptr(), Block :: #rt_block{}) ->
    ok | no_return().
%% ====================================================================
push_nif(_ContainerPtr, _Block) ->
    throw("NIF library not loaded.").


%% fetch_nif/1
%% ====================================================================
%% @doc Fetches block from RTransfer container using NIF library.
%% @end
-spec fetch_nif(ContainerPtr :: container_ptr()) ->
    {ok, #rt_block{}} | {error, Error :: string()} | no_return().
%% ====================================================================
fetch_nif(_ContainerPtr) ->
    throw("NIF library not loaded.").


%% fetch_nif/3
%% ====================================================================
%% @doc Fetches blocks from RTransfer container that matches segment
%% given as offset and size using NIF library.
%% @end
-spec fetch_nif(ContainerPtr :: container_ptr(), Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    {ok, [#rt_block{}]} | {error, Error :: string()} | no_return().
%% ====================================================================
fetch_nif(_ContainerPtr, _Offset, _Size) ->
    throw("NIF library not loaded.").
