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
-export([new/0, new/1, new/2, new/3, delete/1, push/2, pop/1, pop/2]).

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
%% RTransfer block size.
%% @end
-spec new() -> {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
%% ====================================================================
new() ->
    {ok, BlockSize} = application:get_env(?APP_Name, max_rt_block_size),
    new(".", BlockSize).


%% new/1
%% ====================================================================
%% @doc Same as new/0, but allows to register container under given name.
%% @end
-spec new(ContainerName) -> {ok, Pid :: pid()} | ignore | {error, Error :: term()} when
    ContainerName :: {local, Name} | {global, GlobalName} | {via, Module, ViaName},
    Name :: atom(),
    Module :: module(),
    GlobalName :: term(),
    ViaName :: term().
%% ====================================================================
new(ContainerName) ->
    {ok, BlockSize} = application:get_env(?APP_Name, max_rt_block_size),
    new(ContainerName, ".", BlockSize).


%% new/2
%% ====================================================================
%% @doc Creates RTransfer container.
%% @end
-spec new(Prefix :: string(), BlockSize :: integer()) ->
    {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
%% ====================================================================
new(Prefix, BlockSize) ->
    gen_server:start_link(?MODULE, [Prefix, BlockSize], []).


%% new/3
%% ====================================================================
%% @doc Creates RTransfer container and registeres it under given name.
%% @end
-spec new(ContainerName, Prefix :: string(), BlockSize :: integer()) ->
    {ok, Pid :: pid()} | ignore | {error, Error :: term()} when
    ContainerName :: {local, Name} | {global, GlobalName} | {via, Module, ViaName},
    Name :: atom(),
    Module :: module(),
    GlobalName :: term(),
    ViaName :: term().
%% ====================================================================
new(ContainerName, Prefix, BlockSize) ->
    gen_server:start_link(ContainerName, ?MODULE, [Prefix, BlockSize], []).


%% push/2
%% ====================================================================
%% @doc Pushes block on RTransfer container.
%% @end
-spec push(ContainerRef, Block :: #rt_block{}) -> ok when
    ContainerRef :: Name | {Name, Node} | {global, GlobalName} | {via, Module, ViaName} | Pid,
    Pid :: pid(),
    Name :: atom(),
    Node :: node(),
    Module :: module(),
    GlobalName :: term(),
    ViaName :: term().
%% ====================================================================
push(ContainerRef, #rt_block{provider_id = ProviderId} = Block) when is_binary(ProviderId) ->
    push(ContainerRef, Block#rt_block{provider_id = binary_to_list(ProviderId)});

push(ContainerRef, #rt_block{provider_id = ProviderId} = Block) when is_list(ProviderId) ->
    gen_server:cast(ContainerRef, {push, Block}).


%% pop/1
%% ====================================================================
%% @doc Popes block from RTransfer container.
%% @end
-spec pop(ContainerRef) -> {ok, #rt_block{}} | {error, Error :: string()} when
    ContainerRef :: Name | {Name, Node} | {global, GlobalName} | {via, Module, ViaName} | Pid,
    Pid :: pid(),
    Name :: atom(),
    Node :: node(),
    Module :: module(),
    GlobalName :: term(),
    ViaName :: term().
%% ====================================================================
pop(ContainerRef) ->
    pop(ContainerRef, fun erlang:is_process_alive/1).


%% pop/2
%% ====================================================================
%% @doc Popes block from RTransfer container and allows to fillter pids
%% @end
-spec pop(ContainerRef, PidsFilterFunction) -> {ok, #rt_block{}} | {error, Error :: string()} when
    ContainerRef :: Name | {Name, Node} | {global, GlobalName} | {via, Module, ViaName} | Pid,
    PidsFilterFunction :: function(), %% fun(Pid) -> true | false;
    Pid :: pid(),
    Name :: atom(),
    Node :: node(),
    Module :: module(),
    GlobalName :: term(),
    ViaName :: term().
%% ====================================================================
pop(ContainerRef, PidsFilterFunction) ->
    case gen_server:call(ContainerRef, pop) of
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


%% delete/1
%% ====================================================================
%% @doc Deletes RTransfer container.
%% @end
-spec delete(ContainerRef) -> ok | {error, Reason :: term()} when
    ContainerRef :: Name | {Name, Node} | {global, GlobalName} | {via, Module, ViaName} | Pid,
    Pid :: pid(),
    Name :: atom(),
    Node :: node(),
    Module :: module(),
    GlobalName :: term(),
    ViaName :: term().
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
init([Prefix, BlockSize]) ->
    try
        erlang:load_nif(filename:join(Prefix, "c_lib/rt_container_drv"), 0),
        {ok, Container} = init_nif(BlockSize),
        {ok, #state{container = Container}}
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
handle_call(pop, _From, #state{container = Container} = State) ->
    {reply, pop_nif(Container), State};

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
    push_nif(Container, Block),
    {noreply, State};

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
%% @doc Initializes RTransfer container using NIF library.
%% @end
-spec init_nif(BlockSize :: integer()) -> {ok, Container :: term()} | no_return().
%% ====================================================================
init_nif(_BlockSize) ->
    throw("NIF library not loaded.").


%% push_nif/2
%% ====================================================================
%% @doc Pushes block on RTransfer container using NIF library.
%% @end
-spec push_nif(Container :: term(), Block :: #rt_block{}) -> ok | no_return().
%% ====================================================================
push_nif(_Container, _Block) ->
    throw("NIF library not loaded.").


%% pop_nif/1
%% ====================================================================
%% @doc Popes block from RTransfer container using NIF library.
%% @end
-spec pop_nif(Container :: term()) -> {ok, #rt_block{}} | {error, Error :: string()} | no_return().
%% ====================================================================
pop_nif(_Container) ->
    throw("NIF library not loaded.").
