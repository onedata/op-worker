%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% A minimal HTTP file server used to back HTTP (read-only, imported) storages
%%% in tests. It serves static files over plain GET from a per-server document
%%% root, using OTP's built-in inets/httpd.
%%%
%%% The server runs ON the op_worker node (so it is reachable by the HTTP storage
%%% helper from within the provider pod) and is bound to the loopback interface;
%%% the resulting endpoint is `http://127.0.0.1:<port>'. Only the registering
%%% provider reads from it - the other provider obtains the data via Onedata
%%% replication.
%%%
%%% Files are placed with {@link add_file/4}; the relative storage file id is
%%% mapped 1:1 onto a path under the document root (so registering a file with
%%% `storageFileId = <<"/dir/file">>' makes it available at `GET /dir/file').
%%%
%%% NOTE: this module must be added to ?LOAD_MODULES of any suite that uses it,
%%% as the on-node routines are executed on the op_worker node via rpc.
%%% @end
%%%-------------------------------------------------------------------
-module(http_storage_test_server).
-author("Bartosz Walkowicz").

-record(http_test_server, {
    httpd_pid :: pid(),
    doc_root :: file:filename_all(),
    endpoint :: binary()
}).

-opaque handle() :: #http_test_server{}.
-export_type([handle/0]).

-define(SERVER_NAME, "http_storage_test_server").

%% API
-export([start/1, stop/2, stop_all/1]).
-export([add_file/4]).
-export([endpoint/1]).
%% on-node routines (executed on op_worker via rpc)
-export([start_on_node/0, stop_on_node/2, stop_all_on_node/0, add_file_on_node/3]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec start(oct_background:node_selector()) -> handle().
start(ProviderSelector) ->
    {HttpdPid, DocRoot, Port} = opw_test_rpc:call(ProviderSelector, ?MODULE, start_on_node, []),
    #http_test_server{
        httpd_pid = HttpdPid,
        doc_root = DocRoot,
        endpoint = <<"http://127.0.0.1:", (integer_to_binary(Port))/binary>>
    }.


-spec stop(oct_background:node_selector(), handle()) -> ok.
stop(ProviderSelector, #http_test_server{httpd_pid = HttpdPid, doc_root = DocRoot}) ->
    ok = opw_test_rpc:call(ProviderSelector, ?MODULE, stop_on_node, [HttpdPid, DocRoot]).


%% @doc Stops all test http servers lingering on the node (e.g. left over by
%% previous test cases), removing their document roots.
-spec stop_all(oct_background:node_selector()) -> ok.
stop_all(ProviderSelector) ->
    ok = opw_test_rpc:call(ProviderSelector, ?MODULE, stop_all_on_node, []).


-spec add_file(oct_background:node_selector(), handle(), binary(), binary()) -> ok.
add_file(ProviderSelector, #http_test_server{doc_root = DocRoot}, StorageFileId, Content) ->
    ok = opw_test_rpc:call(ProviderSelector, ?MODULE, add_file_on_node, [DocRoot, StorageFileId, Content]).


-spec endpoint(handle()) -> binary().
endpoint(#http_test_server{endpoint = Endpoint}) ->
    Endpoint.


%%%===================================================================
%%% On-node routines
%%%===================================================================


%% @doc Runs on the op_worker node.
-spec start_on_node() -> {pid(), file:filename_all(), inet:port_number()}.
start_on_node() ->
    {ok, _} = application:ensure_all_started(inets),
    UniqueId = integer_to_list(erlang:unique_integer([positive, monotonic])),
    DocRoot = filename:join(["/tmp", "http_storage_test_server", UniqueId]),
    ok = filelib:ensure_path(DocRoot),
    {ok, HttpdPid} = inets:start(httpd, [
        {port, 0},
        {bind_address, {127, 0, 0, 1}},
        {server_name, ?SERVER_NAME},
        {server_root, DocRoot},
        {document_root, DocRoot},
        % serve every file as raw bytes regardless of its (possibly absent) extension
        {mime_types, [{"", "application/octet-stream"}]},
        {mime_type, "application/octet-stream"}
    ]),
    Port = proplists:get_value(port, httpd:info(HttpdPid, [port])),
    {HttpdPid, DocRoot, Port}.


%% @doc Runs on the op_worker node.
-spec stop_on_node(pid(), file:filename_all()) -> ok.
stop_on_node(HttpdPid, DocRoot) ->
    ok = inets:stop(httpd, HttpdPid),
    _ = file:del_dir_r(DocRoot),
    ok.


%% @doc Runs on the op_worker node.
-spec stop_all_on_node() -> ok.
stop_all_on_node() ->
    case lists:keymember(inets, 1, application:which_applications()) of
        false ->
            ok;
        true ->
            lists:foreach(fun(HttpdPid) ->
                case proplists:get_value(server_name, httpd:info(HttpdPid, [server_name])) of
                    ?SERVER_NAME ->
                        DocRoot = proplists:get_value(
                            document_root, httpd:info(HttpdPid, [document_root])
                        ),
                        stop_on_node(HttpdPid, DocRoot);
                    _ ->
                        ok
                end
            end, [Pid || {httpd, Pid} <- inets:services()])
    end.


%% @doc Runs on the op_worker node.
-spec add_file_on_node(file:filename_all(), binary(), binary()) -> ok.
add_file_on_node(DocRoot, StorageFileId, Content) ->
    RelPath = string:trim(binary_to_list(StorageFileId), leading, "/"),
    AbsPath = filename:join(DocRoot, RelPath),
    ok = filelib:ensure_dir(AbsPath),
    ok = file:write_file(AbsPath, Content).
