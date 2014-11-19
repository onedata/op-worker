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
-export([push/2, fetch/4, remove/4]).

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


%% delete/1
%% ====================================================================
%% @doc Deletes RTransfer map.
%% @end
-spec delete(ContainerRef) -> ok | {error, Reason :: term()} when
    ContainerRef :: container_ref().
%% ====================================================================
delete(ContainerRef) ->
    gen_server:call(ContainerRef, delete).


%% push/2
%% ====================================================================
%% @doc Pushes block on RTransfer map.
%% @end
-spec push(ContainerRef, Block :: #rt_block{}) -> ok when
    ContainerRef :: container_ref().
%% ====================================================================
push(ContainerRef, #rt_block{provider_ref = ProviderId} = Block) when is_binary(ProviderId) ->
    push(ContainerRef, Block#rt_block{provider_ref = binary_to_list(ProviderId)});

push(ContainerRef, #rt_block{provider_ref = ProviderId} = Block) when is_list(ProviderId) ->
    gen_server:cast(ContainerRef, {push, Block}).


%% fetch/4
%% ====================================================================
%% @doc Fetches blocks from RTransfer map that matches range
%% given as offset and size using NIF library.
%% @end
-spec fetch(ContainerRef, FileId, Offset, Size) -> {ok, [#rt_block{}]} | {error, Error :: term()} when
    ContainerRef :: container_ref(),
    FileId :: string(),
    Offset :: non_neg_integer(),
    Size :: non_neg_integer().
%% ====================================================================
fetch(ContainerRef, FileId, Offset, Size) ->
    case gen_server:call(ContainerRef, {fetch, FileId, Offset, Size}) of
        {ok, Blocks} ->
            {ok, lists:map(fun(#rt_block{provider_ref = ProviderId} = Block) ->
                Block#rt_block{provider_ref = list_to_binary(ProviderId)}
            end, Blocks)};
        Other ->
            Other
    end.


%% remove/4
%% ====================================================================
%% @doc Removes blocks from RTransfer map that matches range
%% given as offset and size using NIF library.
%% @end
-spec remove(ContainerRef, FileId, Offset, Size) -> ok when
    ContainerRef :: container_ref(),
    FileId :: string(),
    Offset :: non_neg_integer(),
    Size :: non_neg_integer().
%% ====================================================================
remove(ContainerRef, FileId, Offset, Size) ->
    gen_server:cast(ContainerRef, {remove, FileId, Offset, Size}).


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
        {ok, ContainerPtr} = init_nif(BlockSize),
        {ok, #state{container_ptr = ContainerPtr}}
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
handle_call({fetch, FileId, Offset, Size}, _From, #state{container_ptr = ContainerPtr} = State) ->
    {reply, fetch_nif(ContainerPtr, FileId, Offset, Size), State};

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
handle_cast({push, Block}, #state{container_ptr = ContainerPtr} = State) ->
    push_nif(ContainerPtr, Block),
    {noreply, State};

handle_cast({remove, FileId, Offset, Size}, #state{container_ptr = ContainerPtr} = State) ->
    remove_nif(ContainerPtr, FileId, Offset, Size),
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


%% fetch_nif/4
%% ====================================================================
%% @doc Fetches blocks from RTransfer map that matches range
%% given as offset and size using NIF library.
%% @end
-spec fetch_nif(ContainerPtr :: container_ptr(), FileId, Offset, Size) ->
    {ok, [#rt_block{}]} | {error, Error :: term()} | no_return() when
    FileId :: string(),
    Offset :: non_neg_integer(),
    Size :: non_neg_integer().
%% ====================================================================
fetch_nif(_ContainerPtr, _FileId, _Offset, _Size) ->
    throw("NIF library not loaded.").


%% remove_nif/4
%% ====================================================================
%% @doc Removes blocks from RTransfer map that matches range
%% given as offset and size using NIF library.
%% @end
-spec remove_nif(ContainerPtr :: container_ptr(), FileId, Offset, Size) ->
    ok | {error, Error :: term()} | no_return() when
    FileId :: string(),
    Offset :: non_neg_integer(),
    Size :: non_neg_integer().
%% ====================================================================
remove_nif(_ContainerPtr, _FileId, _Offset, _Size) ->
    throw("NIF library not loaded.").
