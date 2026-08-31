%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla, Michał Stanisz
%%% @copyright (C) 2016-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements utility functions for performing storage verification tests.
%%% Storage tests are performed on all cluster nodes. Each storage is checked whether it
%%% is accessible from a node. If a storage is not readonly additionally a read-write test is performed.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_detector).
-author("Krzysztof Trzepla").
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([
    run_diagnostics/4
]).
-export([
    check_storage_access/2,
    create_test_file/2, create_test_file/3,
    write_test_file/3,
    read_test_file/3, check_test_file_content/4,
    remove_test_file/4
]).

-type operation() :: access | create | write | read | remove.
-type diagnostic_opts() :: #{read_write_test := boolean()}.
-type diagnostic_error_details() :: any().

-export_type([diagnostic_opts/0, diagnostic_error_details/0]).

-define(DUMMY_SPACE_DIR_NAME, <<"test_space_name">>).
-define(TEST_FILE_NAME_LEN, op_worker:get_env(storage_test_file_name_size, 32)).
-define(TEST_FILE_CONTENT_LEN, op_worker:get_env(storage_test_file_content_size, 100)).

% flag intended only for acceptance testing, should never be used in the production.
-define(SKIP_STORAGE_DETECTION, op_worker:get_env(skip_storage_detection, false)).

-define(OPERATION_FAILED(Operation, Reason), {operation_failed, Operation, Reason}).

%%%===================================================================
%%% API
%%%===================================================================

-spec run_diagnostics(all_nodes | this_node, helper_spec:t(), luma_config:feed(), diagnostic_opts()) ->
    ok | {errors:error(), diagnostic_error_details()}.
run_diagnostics(all_nodes, HelperSpec, LumaFeed, Opts) ->
    Nodes = consistent_hashing:get_all_nodes(),
    run_diagnostics_on_nodes(Nodes, HelperSpec, LumaFeed, Opts);
run_diagnostics(this_node, HelperSpec, LumaFeed, Opts) ->
    run_diagnostics_on_nodes([node()], HelperSpec, LumaFeed, Opts).


-spec check_storage_access(helper_spec:t(), helper_spec:credentials()) ->
    ok | {error, term()}.
check_storage_access(HelperSpec, StorageCredentials) ->
    Handle = helpers:get_helper_handle(HelperSpec, StorageCredentials),
    ok = helpers:check_storage_availability(Handle).


-spec create_test_file(helper_spec:t(), helper_spec:credentials()) ->
    {ok, helpers:file_id()}.
create_test_file(#helper_spec{name = ?S3_HELPER_NAME, configuration = #{<<"archiveStorage">> := <<"true">>}} = HelperSpec, StorageCredentials) ->
    % S3 storage with archive_storage set to true requires all files to be created in a space directory
    % therefore test file also needs to be created in such a directory.
    create_test_file(HelperSpec, StorageCredentials, ?DUMMY_SPACE_DIR_NAME);
create_test_file(HelperSpec, StorageCredentials) ->
    create_test_file(HelperSpec, StorageCredentials, <<>>).


-spec create_test_file(helper_spec:t(), helper_spec:credentials(), binary()) ->
    {ok, helpers:file_id()}.
create_test_file(HelperSpec, StorageCredentials, SpaceDirName) ->
    FileId = filename:join([SpaceDirName, generate_file_id()]),
    Handle = helpers:get_helper_handle(HelperSpec, StorageCredentials),
    ok = helpers:mknod(Handle, FileId, 8#666, reg),
    {ok, FileId}.


-spec write_test_file(helper_spec:t(), helper_spec:credentials(), helpers:file_id()) ->
    {ok, binary()}.
write_test_file(HelperSpec, StorageCredentials, FileId) ->
    Handle = helpers:get_helper_handle(HelperSpec, StorageCredentials),
    {ok, FileHandle} = helpers:open(Handle, FileId, write),
    Content = random_ascii_lowercase_sequence(?TEST_FILE_CONTENT_LEN),
    {ok, _} = helpers:write(FileHandle, 0, Content),
    ok = helpers:flushbuffer(Handle, FileId, ?TEST_FILE_CONTENT_LEN),
    ok = helpers:release(FileHandle),
    {ok, Content}.


-spec check_test_file_content(helper_spec:t(), helper_spec:credentials(), helpers:file_id(), binary()) ->
    ok.
check_test_file_content(HelperSpec, StorageCredentials, FileId, ExpectedContent) ->
    {ok, ExpectedContent} = read_test_file(HelperSpec, StorageCredentials, FileId),
    ok.


-spec read_test_file(helper_spec:t(), helper_spec:credentials(), helpers:file_id()) ->
    {ok, binary()}.
read_test_file(HelperSpec, StorageCredentials, FileId) ->
    Handle = helpers:get_helper_handle(HelperSpec, StorageCredentials),
    {ok, FileHandle} = helpers:open(Handle, FileId, read),
    {ok, Content} = helpers:read(FileHandle, 0, ?TEST_FILE_CONTENT_LEN),
    ok = helpers:release(FileHandle),
    {ok, Content}.


-spec remove_test_file(helper_spec:t(), helper_spec:credentials(), helpers:file_id(),
    Size :: non_neg_integer()) -> ok | no_return().
remove_test_file(HelperSpec, StorageCredentials, FileId, Size) ->
    Handle = helpers:get_helper_handle(HelperSpec, StorageCredentials),
    case helpers:unlink(Handle, FileId, Size) of
        ok -> ok;
        {error, ?ENOENT} -> ok
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec run_diagnostics_on_nodes([node()], helper_spec:t(), luma_config:feed(), diagnostic_opts()) ->
    ok | {errors:error(), diagnostic_error_details()}.
run_diagnostics_on_nodes(_Nodes, #helper_spec{name = ?NULL_DEVICE_HELPER_NAME}, _LumaFeed, _) ->
    ok;
run_diagnostics_on_nodes(_Nodes, #helper_spec{name = ?HTTP_HELPER_NAME}, _LumaFeed, _) ->
    ok;
run_diagnostics_on_nodes(Nodes, HelperSpec, LumaFeed, Options) ->
    try
        case ?SKIP_STORAGE_DETECTION of
            true ->
                ok;
            false ->
                AdminCredentials = helper_spec:get_credentials(HelperSpec),
                {ok, ExtendedAdminCredentials} = luma:add_helper_specific_fields(
                    ?ROOT_USER_ID, ?ROOT_SESS_ID, AdminCredentials, HelperSpec, LumaFeed
                ),
                run_diagnostics_on_nodes_insecure(Nodes, HelperSpec, ExtendedAdminCredentials, Options)
        end
    catch throw:?OPERATION_FAILED(Operation, Reason) ->
        {?ERR_STORAGE_TEST_FAILED(?err_ctx(), Operation), Reason}
    end.


%% @private
-spec run_diagnostics_on_nodes_insecure([node()], helper_spec:t(), helper_spec:credentials(), diagnostic_opts()) ->
    ok.
run_diagnostics_on_nodes_insecure(Nodes, HelperSpec, StorageCredentials, Opts) ->
    BasicArgList = [[HelperSpec, StorageCredentials] || _N <- Nodes],
    perform_operation(Nodes, access, check_storage_access, BasicArgList),
    case maps:get(read_write_test, Opts) of
        true -> perform_read_write_test(Nodes, BasicArgList);
        false -> ok
    end.


%% @private
-spec perform_read_write_test([node()], [[term()]]) -> ok.
perform_read_write_test(Nodes, BasicArgList) ->
    FileIds = perform_operation(Nodes, create, create_test_file, BasicArgList),
    WriteFileArgList = extend_args(BasicArgList, FileIds),
    % shift nodes by one between operations, so that a node performs operation on file changed previously by other node
    Contents = perform_operation(shift(Nodes), write, write_test_file, WriteFileArgList),
    perform_operation(Nodes, read, check_test_file_content, extend_args(WriteFileArgList, Contents)),
    perform_operation(shift(Nodes), remove, remove_test_file, extend_args(WriteFileArgList, ?TEST_FILE_CONTENT_LEN)),
    ok.


%% @private
-spec perform_operation([node()], operation(), atom(), [[term()]]) -> [term()].
perform_operation(Nodes, Operation, Function, ListOfArgs) ->
    lists:map(fun({Node, Args}) ->
        check_call_result(catch erpc:call(Node, ?MODULE, Function, Args), Operation)
    end, lists:zip(Nodes, ListOfArgs)).


%% @private
-spec extend_args([A], [B] | B) -> [A | B].
extend_args(Args, ExtensionList) when is_list(ExtensionList) ->
    lists:map(fun({PrevArgs, Extension}) ->
        PrevArgs ++ [Extension]
    end, lists:zip(Args, ExtensionList));
extend_args(Args, Extension) ->
    extend_args(Args, lists:duplicate(length(Args), Extension)).


%% @private
-spec check_call_result(ok | {ok, A} | errors:error() | {atom(), {any(), stacktrace()}} | {atom(), any()}, operation()) ->
    ok | A | no_return().
check_call_result(Result, Operation) ->
    case Result of
        ok ->
            ok;
        {ok, R} ->
            R;
        {_Class, {Reason, _Stacktrace}} ->
            throw(?OPERATION_FAILED(Operation, Reason))
    end.


%% @private
-spec generate_file_id() -> binary().
generate_file_id() ->
    file_meta:hidden_file_name(random_ascii_lowercase_sequence(?TEST_FILE_NAME_LEN)).


%% @private
-spec random_ascii_lowercase_sequence(Length :: integer()) -> binary().
random_ascii_lowercase_sequence(Length) ->
    rand:seed(exs1024),
    lists:foldl(fun(_, Acc) ->
        <<Acc/binary, (rand:uniform(26) + 96)>>
    end, <<>>, lists:seq(1, Length)).


%% @private
-spec shift(list()) -> list().
shift([H | List]) ->
    List ++ [H].
