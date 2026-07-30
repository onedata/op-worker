%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
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

-include_lib("inets/include/httpd.hrl").

-record(http_test_server, {
    httpd_pid :: pid(),
    doc_root :: file:filename_all(),
    endpoint :: binary()
}).

-opaque handle() :: #http_test_server{}.
-export_type([handle/0]).

-define(SERVER_NAME, "http_storage_test_server").
%% persistent_term key holding a map of #{<<"/uri/path">> => HttpStatusCode} used
%% to make the server respond with a chosen error status for selected files
-define(INJECTED_ERRORS_KEY, {?MODULE, injected_errors}).

%% API
-export([start/1, stop/2, stop_all/1]).
-export([add_file/4, remove_file/3, set_file_error/4]).
-export([endpoint/1]).
%% inets httpd callback module (runs on the op_worker node, registered first in the
%% modules pipeline so it can short-circuit selected requests with an error status)
-export([do/1]).
%% on-node routines (executed on op_worker via rpc)
-export([
    start_on_node/0, stop_on_node/2, stop_all_on_node/0,
    add_file_on_node/3, remove_file_on_node/2, set_file_error_on_node/2
]).


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


-spec remove_file(oct_background:node_selector(), handle(), binary()) -> ok.
remove_file(ProviderSelector, #http_test_server{doc_root = DocRoot}, StorageFileId) ->
    ok = opw_test_rpc:call(ProviderSelector, ?MODULE, remove_file_on_node, [DocRoot, StorageFileId]).


%% @doc Makes subsequent requests for the given file respond with the chosen HTTP
%% status code (instead of serving the file), to test storage error handling.
-spec set_file_error(oct_background:node_selector(), handle(), binary(), non_neg_integer()) -> ok.
set_file_error(ProviderSelector, #http_test_server{}, StorageFileId, StatusCode) ->
    ok = opw_test_rpc:call(ProviderSelector, ?MODULE, set_file_error_on_node, [StorageFileId, StatusCode]).


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
        % default httpd module pipeline with our error-injecting module prepended so
        % it can short-circuit selected requests (see do/1); leaving the rest of the
        % list as the inets default preserves the regular static-file/HEAD serving
        {modules, [
            ?MODULE,
            mod_alias, mod_auth, mod_esi, mod_actions, mod_cgi,
            mod_dir, mod_get, mod_head, mod_log, mod_disk_log
        ]},
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
    clear_injected_errors().


%% @doc Runs on the op_worker node.
-spec stop_all_on_node() -> ok.
stop_all_on_node() ->
    clear_injected_errors(),
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


%% @doc Runs on the op_worker node.
-spec remove_file_on_node(file:filename_all(), binary()) -> ok.
remove_file_on_node(DocRoot, StorageFileId) ->
    RelPath = string:trim(binary_to_list(StorageFileId), leading, "/"),
    AbsPath = filename:join(DocRoot, RelPath),
    _ = file:delete(AbsPath),
    ok.


%% @doc Runs on the op_worker node.
-spec set_file_error_on_node(binary(), non_neg_integer()) -> ok.
set_file_error_on_node(StorageFileId, StatusCode) ->
    CurrentErrors = persistent_term:get(?INJECTED_ERRORS_KEY, #{}),
    persistent_term:put(
        ?INJECTED_ERRORS_KEY, CurrentErrors#{storage_file_id_to_uri_key(StorageFileId) => StatusCode}
    ),
    ok.


%%%===================================================================
%%% inets httpd callback
%%%===================================================================


%% @doc
%% Runs on the op_worker node, invoked by inets httpd for every request before the
%% static-file modules. If an error status has been injected for the requested path
%% (see set_file_error/4) it responds with that status; otherwise it lets the
%% default module pipeline serve the file.
%% @end
-spec do(#mod{}) -> {proceed, list()}.
do(#mod{request_uri = RequestUri, data = Data}) ->
    InjectedErrors = persistent_term:get(?INJECTED_ERRORS_KEY, #{}),
    case maps:find(request_uri_to_key(RequestUri), InjectedErrors) of
        {ok, StatusCode} ->
            Body = "injected storage error",
            {proceed, [{response, {response,
                [{code, StatusCode}, {content_length, integer_to_list(length(Body))}],
                Body
            }} | Data]};
        error ->
            {proceed, Data}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec clear_injected_errors() -> ok.
clear_injected_errors() ->
    _ = persistent_term:erase(?INJECTED_ERRORS_KEY),
    ok.


%% @private
-spec request_uri_to_key(string()) -> binary().
request_uri_to_key(RequestUri) ->
    [PathOnly | _] = string:split(RequestUri, "?"),
    list_to_binary(PathOnly).


%% @private
-spec storage_file_id_to_uri_key(binary()) -> binary().
storage_file_id_to_uri_key(StorageFileId) ->
    case iolist_to_binary(StorageFileId) of
        <<"/", _/binary>> = Path -> Path;
        Path -> <<"/", Path/binary>>
    end.
