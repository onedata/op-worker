%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% {@module} provides API for fetching data from a remote provider.
%%% @end
%%%-------------------------------------------------------------------
-module(rtransfer).
-author("Konrad Zemek").

-include("modules/rtransfer/rtransfer.hrl").

-type address() :: {inet:hostname() | inet:ip_address(), inet:port_number()}.
-type ref() :: #request_transfer{}.
-type error_reason() ::
canceled | {connection | storage | other, Reason :: any()}.

-type get_nodes_fun() ::
fun((ProviderId :: binary()) ->
    [address()]).

-type open_fun() ::
fun((FileUUID :: binary(), read | write) ->
    {ok, Handle :: term()} | {error, Reason :: any()}).

-type close_fun() ::
fun((Handle :: term()) -> ok).

-type read_fun() ::
fun((Handle :: term(), Offset :: non_neg_integer(), Size :: pos_integer()) ->
    {ok, NewHandle1 :: term(), Data :: binary()} |
    {error, NewHandle2 :: term(), Reason :: any()}).

-type write_fun() ::
fun((Handle :: term(), Offset :: non_neg_integer(), Data :: binary()) ->
    {ok, NewHandle1 :: term(), Wrote :: non_neg_integer()} |
    {error, NewHandle2 :: term(), Reason :: any()}).

-type notify_fun() ::
fun((Ref :: ref(), Offset :: non_neg_integer(), Size :: pos_integer()) ->
    ok).

-type on_complete_fun() ::
fun((Ref :: ref(),
{ok, Size :: non_neg_integer()} | {error, error_reason()}) -> ok).

-type opt() ::
{block_size, non_neg_integer()} |
{retry, non_neg_integer()} |
{bind, [inet:ip_address()]} |
{get_nodes_fun, get_nodes_fun()} |
{open_fun, open_fun()} |
{read_fun, read_fun()} |
{write_fun, write_fun()} |
{close_fun, close_fun()} |
{ranch_opts,
    [
    {num_acceptors, non_neg_integer()} |
    {transport, module()} |
    {trans_opts, any()}
    ]}.

%% API
-export_type([ref/0, opt/0, notify_fun/0, on_complete_fun/0]).
-export([start_link/1, prepare_request/4, fetch/3, cancel/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts a rtransfer gen_server on this node.
%% The server will periodically send heartbeats to a leader node
%% (registered as `{global, rtransfer}'). {@module} server handles
%% requests from the leader and listens for network connections on the
%% current node to handle requests from remote providers.
%%
%% If the started rtransfer is elected as leader, it will store a list
%% of active nodes and distribute requests between them. All methods
%% in {@module} interface with the leader.
%%
%% Options:<br/>
%% `block_size' - {@module} will split big requests and attempt to
%% merge adjacent ones so that the actual fetch requests ask for
%% exactly `block_size' bytes. Default: 100 MB<br/>
%% `get_nodes_fun' - function returning a set of gateway nodes for
%% a remote provider.<br/>
%% `open_fun' - function returning a file handle passed to read and
%% write functions.<br/>
%% `read_fun' - function returning data for given offset and size.<br/>
%% `write_fun' - function saving data for a given offset.<br/>
%% `close_fun' - function closing the file handle obtained through
%% open function.<br/>
%% `ranch_opts' - options passed to {@link ranch:start_listener/6}.
%% @end
%%--------------------------------------------------------------------
-spec start_link(RtransferOpts :: [opt()]) -> {ok, pid()}.
start_link(RtransferOpts) ->
    gen_server2:start({global, rtransfer}, rtransfer_server, RtransferOpts, []),
    gen_server2:start_link({local, gateway}, gateway, RtransferOpts, []).

%%--------------------------------------------------------------------
%% @doc
%% Prepares a term that uniquely references a request.
%% @end
%%--------------------------------------------------------------------
-spec prepare_request(ProviderId :: binary(), FileUUID :: binary(),
    Offset :: non_neg_integer(), Size :: pos_integer()) -> Ref :: ref().
prepare_request(ProviderId, FileUUID, Offset, Size) ->
    #request_transfer{provider_id = ProviderId, file_id = FileUUID,
        offset = Offset, size = Size}.

%%--------------------------------------------------------------------
%% @doc
%% Starts a fetch request.
%% `Notify' will be called for completed sub-transfers. Although
%% sub-transfers can be completed out of order, {@module} guarantees
%% that `Notify' is called for subsequent blocks.
%% `OnComplete' will be called whenever a full transfer is completed,
%% either with error or success.
%% Returns updated ref.
%% @end
%%--------------------------------------------------------------------
-spec fetch(Ref :: ref(), Notify :: notify_fun(),
    OnComplete :: on_complete_fun()) -> ref().
fetch(Ref, Notify, OnComplete) ->
    NewRef = Ref#request_transfer{notify = Notify, on_complete = OnComplete},
    gen_server2:cast({global, rtransfer}, NewRef),
    NewRef.

%%--------------------------------------------------------------------
%% @doc
%% Cancels a fetch request.
%% Due to asynchronous/parallel nature of transfers it's not
%% guaranteed ''when'' the request will actually be cancelled;
%% particularly, a request can be fulfilled before it's cancelled.
%%
%% Cancelled requests will result in `OnComplete' called with
%% `{error, canceled}' status.
%% @end
%%--------------------------------------------------------------------
-spec cancel(Ref :: ref()) -> ok.
cancel(_Ref) ->
    ok.

%% TODO: authorization! can the connected provider read this file?
%% To know we need 1) ProviderId 2) A callback (ProvID, FileUUID) -> allowed?
%% How do we get ProviderId without requiring SSL?
%% TODO: binding! to specific endpoints
%% TODO: defaults! for block_size and (not yet added) heartbeat & timeout & connection_timeout
