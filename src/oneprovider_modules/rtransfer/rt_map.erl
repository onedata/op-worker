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
-module(rt_map).

-include("registered_names.hrl").
-include("oneprovider_modules/rtransfer/rt_map.hrl").
-include("oneprovider_modules/rtransfer/rt_container.hrl").

%% API
-export([new/0, new/1, new/2, new/3, delete/1]).
-export([push/2, fetch/3, remove/3]).

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
%% @doc Creates RTransfer map with default prefix and maximal
%% RTransfer block size.
%% @end
-spec new() -> {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
%% ====================================================================
new() ->
    {ok, BlockSize} = application:get_env(?APP_Name, max_rt_block_size),
    new(".", BlockSize).


%% new/1
%% ====================================================================
%% @doc Same as new/0, but allows to register queue under given name.
%% @end
-spec new(ContainerName) -> {ok, Pid :: pid()} | ignore | {error, Error :: term()} when
    ContainerName :: container_name().
%% ====================================================================
new(ContainerName) ->
    {ok, BlockSize} = application:get_env(?APP_Name, max_rt_block_size),
    new(ContainerName, ".", BlockSize).


%% new/2
%% ====================================================================
%% @doc Creates RTransfer map.
%% @end
-spec new(Prefix :: string(), BlockSize :: integer()) ->
    {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
%% ====================================================================
new(Prefix, BlockSize) ->
    gen_server:start_link(?MODULE, [Prefix, BlockSize], []).


%% new/3
%% ====================================================================
%% @doc Creates RTransfer map and registeres it under given name.
%% @end
-spec new(ContainerName, Prefix :: string(), BlockSize :: integer()) ->
    {ok, Pid :: pid()} | ignore | {error, Error :: term()} when
    ContainerName :: container_name().
%% ====================================================================
new(ContainerName, Prefix, BlockSize) ->
    gen_server:start_link(ContainerName, ?MODULE, [Prefix, BlockSize], []).


%% push/2
%% ====================================================================
%% @doc Pushes block on RTransfer map.
%% @end
-spec push(ContainerRef, Block :: #rt_block{}) -> ok when
    ContainerRef :: container_ref().
%% ====================================================================
push(ContainerRef, #rt_block{provider_id = ProviderId} = Block) when is_binary(ProviderId) ->
    push(ContainerRef, Block#rt_block{provider_id = binary_to_list(ProviderId)});

push(ContainerRef, #rt_block{provider_id = ProviderId} = Block) when is_list(ProviderId) ->
    gen_server:cast(ContainerRef, {push, Block}).


%% fetch/3
%% ====================================================================
%% @doc Fetches blocks from RTransfer map that matches range
%% given as offset and size using NIF library.
%% @end
-spec fetch(ContainerRef, Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    {ok, [#rt_block{}]} | {error, Error :: term()} | no_return() when
    ContainerRef :: container_ref().
%% ====================================================================
fetch(ContainerRef, Offset, Size) ->
    case gen_server:call(ContainerRef, {fetch, Offset, Size}) of
        {ok, #rt_block{provider_id = ProviderId} = Block} ->
            {ok, Block#rt_block{provider_id = list_to_binary(ProviderId)}};
        Other ->
            Other
    end.


%% remove/3
%% ====================================================================
%% @doc Removes blocks from RTransfer map that matches range
%% given as offset and size using NIF library.
%% @end
-spec remove(ContainerRef, Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    {ok, [#rt_block{}]} | {error, Error :: term()} | no_return() when
    ContainerRef :: container_ref().
%% ====================================================================
remove(ContainerRef, Offset, Size) ->
    gen_server:call(ContainerRef, {remove, Offset, Size}).


%% delete/1
%% ====================================================================
%% @doc Deletes RTransfer map.
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
init([Prefix, BlockSize]) ->
    try
        erlang:load_nif(filename:join(Prefix, "c_lib/rt_map_drv"), 0),
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
handle_call({fetch, Offset, Size}, _From, #state{container = Container} = State) ->
    {reply, {ok, fetch_nif(Container, Offset, Size)}, State};

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

handle_cast({remove, Offset, Size}, #state{container = Container} = State) ->
    remove_nif(Container, Offset, Size),
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

%% init_nif/1
%% ====================================================================
%% @doc Initializes RTransfer map using NIF library.
%% @end
-spec init_nif(BlockSize :: integer()) ->
    {ok, ContainerPtr :: container_ptr()} | no_return().
%% ====================================================================
init_nif(_BlockSize) ->
    throw("NIF library not loaded.").


%% push_nif/2
%% ====================================================================
%% @doc Pushes block on RTransfer map using NIF library.
%% @end
-spec push_nif(ContainerPtr :: container_ptr(), Block :: #rt_block{}) ->
    ok | no_return().
%% ====================================================================
push_nif(_ContainerPtr, _Block) ->
    throw("NIF library not loaded.").


%% fetch_nif/3
%% ====================================================================
%% @doc Fetches blocks from RTransfer map that matches range
%% given as offset and size using NIF library.
%% @end
-spec fetch_nif(ContainerPtr :: container_ptr(), Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    {ok, [#rt_block{}]} | {error, Error :: term()} | no_return().
%% ====================================================================
fetch_nif(_ContainerPtr, _Offset, _Size) ->
    throw("NIF library not loaded.").


%% remove_nif/3
%% ====================================================================
%% @doc Removes blocks from RTransfer map that matches range
%% given as offset and size using NIF library.
%% @end
-spec remove_nif(ContainerPtr :: container_ptr(), Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    {ok, [#rt_block{}]} | {error, Error :: term()} | no_return().
%% ====================================================================
remove_nif(_ContainerPtr, _Offset, _Size) ->
    throw("NIF library not loaded.").
