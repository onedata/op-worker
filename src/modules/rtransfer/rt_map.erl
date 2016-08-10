%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module allows for management of RTransfer map.
%%% @end
%%%-------------------------------------------------------------------
-module(rt_map).
-author("Krzysztof Trzepla").
-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules/rtransfer/rt_container.hrl").

-on_load(load_nif/0).

%% API
-export([new/0, new/1, delete/1]).
-export([put/2, get/4, remove/4]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

%% gen_server state
%% * container - pointer to container resource created as a call to rt_container:init_nif() function
-record(state, {container_ptr}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates RTransfer map with default prefix and maximal
%% RTransfer block size.
%% @end
%%--------------------------------------------------------------------
-spec new() -> {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
new() ->
    gen_server:start_link(?MODULE, [], []).


%%--------------------------------------------------------------------
%% @doc
%% Creates RTransfer map and registeres it under given name.
%% @end
%%--------------------------------------------------------------------
-spec new(ContainerName :: container_name()) ->
    {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
new(ContainerName) ->
    gen_server:start_link(ContainerName, ?MODULE, [], []).


%%--------------------------------------------------------------------
%% @doc
%% Deletes RTransfer map.
%% @end
%%--------------------------------------------------------------------
-spec delete(ContainerRef) -> ok | {error, Reason :: term()} when
    ContainerRef :: container_ref().
%%--------------------------------------------------------------------
delete(ContainerRef) ->
    gen_server:call(ContainerRef, delete).


%%--------------------------------------------------------------------
%% @doc
%% Puts block on RTransfer map.
%% @end
%%--------------------------------------------------------------------
-spec put(ContainerRef, Block :: #rt_block{}) -> ok when
    ContainerRef :: container_ref().
put(ContainerRef, Block) ->
    gen_server:cast(ContainerRef, {put, Block}).


%%--------------------------------------------------------------------
%% @doc
%% Gets blocks from RTransfer map that matches range
%% given as offset and size using NIF library.
%% @end
%%--------------------------------------------------------------------
-spec get(ContainerRef, FileId, Offset, Size) -> {ok, [#rt_block{}]} | {error, Error :: term()} when
    ContainerRef :: container_ref(),
    FileId :: binary(),
    Offset :: non_neg_integer(),
    Size :: non_neg_integer().
get(ContainerRef, FileId, Offset, Size) ->
    gen_server:call(ContainerRef, {get, FileId, Offset, Size}).


%%--------------------------------------------------------------------
%% @doc
%% Removes blocks from RTransfer map that matches range
%% given as offset and size using NIF library.
%% @end
%%--------------------------------------------------------------------
-spec remove(ContainerRef, FileId, Offset, Size) -> ok when
    ContainerRef :: container_ref(),
    FileId :: binary(),
    Offset :: non_neg_integer(),
    Size :: non_neg_integer().
remove(ContainerRef, FileId, Offset, Size) ->
    gen_server:cast(ContainerRef, {remove, FileId, Offset, Size}).


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
init([]) ->
    try
        {ok, ContainerPtr} = init_nif(),
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
handle_call({get, FileId, Offset, Size}, _From, #state{container_ptr = ContainerPtr} = State) ->
    {reply, get_nif(ContainerPtr, FileId, Offset, Size), State};

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
handle_cast({put, Block}, #state{container_ptr = ContainerPtr} = State) ->
    put_nif(ContainerPtr, Block),
    {noreply, State};

handle_cast({remove, FileId, Offset, Size}, #state{container_ptr = ContainerPtr} = State) ->
    remove_nif(ContainerPtr, FileId, Offset, Size),
    {noreply, State};

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
%% Loads RTransfer map NIF library.
%% @end
%%--------------------------------------------------------------------
-spec load_nif() -> ok | {error, Reason :: term()}.
load_nif() ->
    LibName = "rt_map_drv",
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
-spec init_nif() ->
    {ok, ContainerPtr :: container_ptr()} | no_return().
init_nif() ->
    erlang:nif_error(nif_library_not_loaded).


%%--------------------------------------------------------------------
%% @doc
%% Puts block on RTransfer map using NIF library.
%% @end
%%--------------------------------------------------------------------
-spec put_nif(ContainerPtr :: container_ptr(), Block :: #rt_block{}) ->
    ok | no_return().
put_nif(_ContainerPtr, _Block) ->
    erlang:nif_error(nif_library_not_loaded).


%%--------------------------------------------------------------------
%% @doc
%% Gets blocks from RTransfer map that matches range
%% given as offset and size using NIF library.
%% @end
%%--------------------------------------------------------------------
-spec get_nif(ContainerPtr :: container_ptr(), FileId, Offset, Size) ->
    {ok, [#rt_block{}]} | {error, Error :: term()} | no_return() when
    FileId :: binary(),
    Offset :: non_neg_integer(),
    Size :: non_neg_integer().
get_nif(_ContainerPtr, _FileId, _Offset, _Size) ->
    erlang:nif_error(nif_library_not_loaded).


%%--------------------------------------------------------------------
%% @doc
%% Removes blocks from RTransfer map that matches range
%% given as offset and size using NIF library.
%% @end
%%--------------------------------------------------------------------
-spec remove_nif(ContainerPtr :: container_ptr(), FileId, Offset, Size) ->
    ok | {error, Error :: term()} | no_return() when
    FileId :: binary(),
    Offset :: non_neg_integer(),
    Size :: non_neg_integer().
remove_nif(_ContainerPtr, _FileId, _Offset, _Size) ->
    erlang:nif_error(nif_library_not_loaded).
