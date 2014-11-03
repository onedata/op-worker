%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: @TODO: write me
%% @end
%% ===================================================================

-module(gateway_protocol).
-author("Konrad Zemek").
-behavior(ranch_protocol).
-behavior(gen_server).

-record(gwproto_state, {
    socket,
    transport,
    ok,
    closed,
    error
}).

-define(connection_close_timeout, timer:minutes(1)).

-include("gwproto_pb.hrl").
-include("oneprovider_modules/dao/dao_vfs.hrl").
-include("oneprovider_modules/gateway/gateway.hrl").
-include("oneprovider_modules/gateway/registered_names.hrl").

%% ranch_protocol callbacks
-export([start_link/4, init/4]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================

start_link(Ref, Socket, Transport, Opts) ->
    proc_lib:start_link(?MODULE, init, [Ref, Socket, Transport, Opts]).


init(Ref, Socket, Transport, _Opts) ->
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [{active, once}, {packet, 4}]),
    {Ok, Closed, Error} = Transport:messages(),
    gen_server:enter_loop(?MODULE, [], #gwproto_state{
        socket = Socket,
        transport = Transport,
        ok = Ok,
        closed = Closed,
        error = Error
    }).


-spec init(State) -> Result when
    Result :: {ok,State} | {ok,State,Timeout} | {ok,State,hibernate}
    | {stop,Reason} | ignore,
    State :: #gwproto_state{},
    Timeout :: timeout(),
    Reason :: term().
init(State) ->
    {ok, State, ?connection_close_timeout}.


-spec handle_call(Request, From, State) -> Result when
    Request :: term(),
    From :: {pid(),any()},
    State :: #gwproto_state{},
    Result :: {reply,Reply,NewState} | {reply,Reply,NewState,Timeout}
    | {reply,Reply,NewState,hibernate}
    | {noreply,NewState} | {noreply,NewState,Timeout}
    | {noreply,NewState,hibernate}
    | {stop,Reason,Reply,NewState} | {stop,Reason,NewState},
    Reply :: term(),
    NewState :: term(),
    Timeout :: timeout(),
    Reason :: term().
handle_call(_Request, _From, State) ->
    ?log_call(_Request),
    {noreply, State, ?connection_close_timeout}.


-spec handle_cast(Request, State) -> Result when
    Request :: term(),
    State :: #gwproto_state{},
    Result :: {noreply,NewState} | {noreply,NewState,Timeout}
    | {noreply,NewState,hibernate}
    | {stop,Reason,NewState},
    NewState :: term(),
    Timeout :: timeout(),
    Reason :: term().
handle_cast(_Request, State) ->
    ?log_call(_Request),
    {noreply, State, ?connection_close_timeout}.


-spec handle_info(Info, State) -> Result when
    Info :: timeout | term(),
    State :: #gwproto_state{},
    Result :: {noreply,NewState} | {noreply,NewState,Timeout}
    | {noreply,NewState,hibernate}
    | {stop,Reason,NewState},
    NewState :: term(),
    Timeout :: timeout(),
    Reason :: normal | term().
handle_info({Ok, Socket, Data}, State) when Ok =:= State#gwproto_state.ok ->
    #gwproto_state{transport = Transport} = State,
    ok = Transport:setopts(Socket, [{active, once}]),

    #fetchrequest{file_id = FileId, offset = Offset, size = Size} = gwproto_pb:decode_fetchrequest(Data),

%%     #file_location{storage_uuid = StorageUUID, storage_file_id = StorageFileId} = fslogic_file:get_file_local_location(FileId),
%%     Storage = fslogic_objects:get_storage({uuid, StorageUUID}),
%%     #storage_info{default_storage_helper = StorageHelper} = Storage,
%%     {ok, Bytes} = storage_files_manager:read(StorageHelper, StorageFileId, Offset, Size),

    Hash = gateway:compute_request_hash(Data),
    Bytes = <<"lalala">>,
    Reply = gwproto_pb:encode_fetchreply(#fetchreply{request_hash = Hash, content = Bytes}),
    ok = Transport:send(Socket, Reply),
    {noreply, State, ?connection_close_timeout};

handle_info({Closed, _Socket}, State) when Closed =:= State#gwproto_state.closed ->
    {stop, normal, State};

handle_info({Error, _Socket, Reason}, State) when Error =:= State#gwproto_state.error ->
    {stop, Reason, State};

handle_info(timeout, State) ->
    {stop, normal, State};

handle_info(_Request, State) ->
    ?log_call(_Request),
    {noreply, State, ?connection_close_timeout}.


-spec terminate(Reason, State) -> IgnoredResult when
    Reason :: normal | shutdown | {shutdown,term()} | term(),
    State :: #gwproto_state{},
    IgnoredResult :: any().
terminate(_Reason, State) ->
    ?log_terminate(_Reason, State),
    #gwproto_state{transport = Transport, socket = Socket} = State,
    Transport:close(Socket).


-spec code_change(OldVsn, State, Extra) -> {ok, NewState} | {error, Reason} when
    OldVsn :: Vsn | {down, Vsn},
    Vsn :: term(),
    State :: #gwproto_state{},
    Extra :: term(),
    NewState :: #gwproto_state{},
    Reason :: term().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================
