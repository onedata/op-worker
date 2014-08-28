%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This is a cdmi handler module providing basic operations on
%% cdmi containers
%% ===================================================================
-module(cdmi_container).

-include("veil_modules/control_panel/cdmi.hrl").
-include("veil_modules/control_panel/cdmi_capabilities.hrl").
-include("veil_modules/control_panel/cdmi_container.hrl").

%% API
-export([allowed_methods/2, malformed_request/2, resource_exists/2, content_types_provided/2, content_types_accepted/2,delete_resource/2]).
-export([get_cdmi_container/2, put_cdmi_container/2, put_binary/2]).

%% allowed_methods/2
%% ====================================================================
%% @doc
%% Returns binary list of methods that are allowed (i.e GET, PUT, DELETE).
%% @end
-spec allowed_methods(req(), #state{}) -> {[binary()], req(), #state{}}.
%% ====================================================================
allowed_methods(Req, State) ->
    {[<<"PUT">>, <<"GET">>, <<"DELETE">>], Req, State}.

%% malformed_request/2
%% ====================================================================
%% @doc
%% Checks if request contains all mandatory fields and their values are set properly
%% depending on requested operation
%% @end
-spec malformed_request(req(), #state{}) -> {boolean(), req(), #state{}} | no_return().
%% ====================================================================
malformed_request(Req, #state{cdmi_version = Version, method = <<"PUT">>} = State) when is_binary(Version) ->
    {<<"application/cdmi-container">>, Req2} = cowboy_req:header(<<"content-type">>, Req),
    {false, Req2, State};
malformed_request(Req, State) ->
    {false, Req, State}.

%% resource_exists/2
%% ====================================================================
%% @doc Determines if resource, that can be obtained from state, exists.
-spec resource_exists(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
resource_exists(Req,State = #state{filepath = Filepath}) ->
    case logical_files_manager:getfileattr(Filepath) of
        {ok, #fileattributes{type = "DIR"} = Attr} -> {true, Req, State#state{attributes = Attr}};
        _ -> {false, Req, State}
    end.

%% content_types_provided/2
%% ====================================================================
%% @doc
%% Returns content types that can be provided and what functions should be used to process the request.
%% Before adding new content type make sure that adequate routing function
%% exists in cdmi_handler
%% @end
-spec content_types_provided(req(), #state{}) -> {[{ContentType,Method}], req(), #state{}} when
    ContentType :: binary(),
    Method :: atom().
%% ====================================================================
content_types_provided(Req, State) ->
    {[
        {<<"application/cdmi-container">>, get_cdmi_container}
    ], Req, State}.

%% content_types_accepted/2
%% ====================================================================
%% @doc
%% Returns content-types that are accepted and what
%% functions should be used to process the requests.
%% Before adding new content type make sure that adequate routing function
%% exists in cdmi_handler
%% @end
-spec content_types_accepted(req(), #state{}) -> {[{ContentType,Method}], req(), #state{}} when
    ContentType :: binary(),
    Method :: atom().
%% ====================================================================
content_types_accepted(Req, #state{cdmi_version = undefined} = State) ->
    {[
        {'*', put_binary}
    ], Req, State};
content_types_accepted(Req, State) ->
    {[
        {<<"application/cdmi-container">>, put_cdmi_container}
    ], Req, State}.

%% delete_resource/3
%% ====================================================================
%% @doc Deletes the resource. Returns whether the deletion was successful.
-spec delete_resource(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
delete_resource(Req, #state{filepath = Filepath} = State) ->
    case is_group_dir(Filepath) of
        false ->
            fs_remove_dir(Filepath),
            {true, Req, State};
        true -> cdmi_error:error_reply(Req, State, ?error_forbidden_code, "Deleting group dir, which is forbidden",[])
    end.

%% ====================================================================
%% Content type callbacks
%% ====================================================================
%% registered in content_types_provided/content_types_accepted and present
%% in main cdmi_handler. They can handle get/put requests depending on content type.
%% ====================================================================

%% get_cdmi_container/2
%% ====================================================================
%% @doc Callback function for cdmi container GET operation (list dir)
-spec get_cdmi_container(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
get_cdmi_container(Req, #state{opts = Opts} = State) ->
    DirCdmi = prepare_container_ans(case Opts of [] -> ?default_get_dir_opts; _ -> Opts end, State),
    Response = rest_utils:encode_to_json({struct, DirCdmi}),
    {Response, Req, State}.

%% put_cdmi_container/2
%% ====================================================================
%% @doc Callback function for cdmi container PUT operation (create dir)
-spec put_cdmi_container(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
put_cdmi_container(Req, #state{filepath = Filepath} = State) ->
    case logical_files_manager:mkdir(Filepath) of
        ok -> %todo check given body
            case logical_files_manager:getfileattr(Filepath) of
                {ok, Attr} ->
                    Response = rest_utils:encode_to_json(
                        {struct, prepare_container_ans(?default_get_dir_opts, State#state{attributes = Attr})}),
                    Req2 = cowboy_req:set_resp_body(Response, Req),
                    {true, Req2, State};
                Error ->
                    logical_files_manager:rmdir(Filepath),
                    cdmi_error:error_reply(Req, State, ?error_forbidden_code, "Cannot get dir attrs: ~p",[Error])
            end;
        {error, dir_exists} -> cdmi_error:error_reply(Req, State, ?error_conflict_code, "Dir creation conflict",[]);
        {logical_file_system_error, "enoent"} -> cdmi_error:error_reply(Req, State, ?error_not_found_code, "Parent dir not found",[]);
        Error -> cdmi_error:error_reply(Req, State, ?error_forbidden_code, "Dir creation error: ~p",[Error])
    end.

%% put_binary/2
%% ====================================================================
%% @doc Callback function for cdmi container PUT operation with non-cdmi
%% body content-type.
%% @end
-spec put_binary(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
put_binary(Req, #state{filepath = Filepath} = State) ->
    case logical_files_manager:mkdir(Filepath) of
        ok -> {true, Req, State};
        {error, dir_exists} -> cdmi_error:error_reply(Req, State, ?error_conflict_code, "Dir creation conflict",[]);
        {logical_file_system_error, "enoent"} -> cdmi_error:error_reply(Req, State, ?error_not_found_code, "Parent dir not found",[]);
        Error -> cdmi_error:error_reply(Req, State, ?error_forbidden_code, "Dir creation error: ~p",[Error])
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% prepare_container_ans/2
%% ====================================================================
%% @doc Prepares proplist formatted answer with field names from given list of binaries
-spec prepare_container_ans([FieldName :: binary()], #state{}) -> [{FieldName :: binary(), Value :: term()}].
%% ====================================================================
prepare_container_ans([], _State) ->
    [];
prepare_container_ans([<<"objectType">> | Tail], State) ->
    [{<<"objectType">>, <<"application/cdmi-container">>} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"objectID">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"objectID">>, cdmi_id:uuid_to_objectid(logical_files_manager:get_uuid_by_filepath(Filepath))} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"objectName">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"objectName">>, list_to_binary([filename:basename(Filepath), "/"])} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"parentURI">> | Tail], #state{filepath = "/"} = State) ->
    prepare_container_ans(Tail, State);
prepare_container_ans([<<"parentURI">> | Tail], #state{filepath = Filepath} = State) ->
    ParentURI = case fslogic_path:strip_path_leaf(Filepath) of
                    "/" -> <<"/">>;
                    Other -> list_to_binary(Other)
                end,
    [{<<"parentURI">>, ParentURI} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"parentID">> | Tail], #state{filepath = "/"} = State) ->
    prepare_container_ans(Tail, State);
prepare_container_ans([<<"parentID">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"parentID">>, cdmi_id:uuid_to_objectid(logical_files_manager:get_uuid_by_filepath(fslogic_path:strip_path_leaf(Filepath)))} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"capabilitiesURI">> | Tail], State) ->
    [{<<"capabilitiesURI">>, list_to_binary(?container_capability_path)} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"completionStatus">> | Tail], State) ->
    [{<<"completionStatus">>, <<"Complete">>} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"metadata">> | Tail], #state{attributes = Attrs} = State) ->
    [{<<"metadata">>, rest_utils:prepare_metadata(Attrs)} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"children">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"children">>, [list_to_binary(Path) || Path <- rest_utils:list_dir(Filepath)]} | prepare_container_ans(Tail, State)];
prepare_container_ans([_Other | Tail], State) ->
    prepare_container_ans(Tail, State).

%% fs_remove/1
%% ====================================================================
%% @doc Removes given file/dir from filesystem and db. In case of dir, it's
%% done recursively.
%% @end
-spec fs_remove(Path :: string()) -> Result when
    Result :: ok | {ErrorGeneral :: atom(), ErrorDetail :: term()}.
%% ====================================================================
fs_remove(Path) ->
    {ok, FA} = logical_files_manager:getfileattr(Path),
    case FA#fileattributes.type of
        "DIR" -> fs_remove_dir(Path);
        "REG" -> logical_files_manager:delete(Path)
    end.

%% fs_remove_dir/1
%% ====================================================================
%% @doc Removes given dir with all files and subdirectories.
%% @end
-spec fs_remove_dir(DirPath :: string()) -> Result when
    Result :: ok | {ErrorGeneral :: atom(), ErrorDetail :: term()}.
%% ====================================================================
fs_remove_dir(DirPath) ->
    case is_group_dir(DirPath) of
        true -> ok;
        false ->
            ItemList = fs_list_dir(DirPath),
            lists:foreach(fun(Item) -> fs_remove(filename:join(DirPath, Item)) end, ItemList),
            logical_files_manager:rmdir(DirPath)
    end.

%% fs_list_dir/1
%% ====================================================================
%% @doc @equiv fs_list_dir(Dir, 0, 10, [])
-spec fs_list_dir(Dir :: string()) -> [string()].
%% ====================================================================
fs_list_dir(Dir) ->
    fs_list_dir(Dir, 0, 10, []).

%% fs_list_dir/4
%% ====================================================================
%% @doc Lists all childrens of given dir, starting from offset and with initial
%% chunk size set to 'Count'
-spec fs_list_dir(Dir :: string(), Offset :: integer(), Count :: integer(), Result :: [string()]) -> [string()].
%% ====================================================================
fs_list_dir(Path, Offset, Count, Result) ->
    case logical_files_manager:ls(Path, Count, Offset) of
        {ok, FileList} ->
            case length(FileList) of
                Count -> fs_list_dir(Path, Offset + Count, Count * 10, Result ++ FileList);
                _ -> Result ++ FileList
            end;
        _ ->
            {error, not_a_dir}
    end.

%% is_group_dir/1
%% ====================================================================
%% @doc Returns true when Path points to group directory (or groups root directory)
-spec is_group_dir(Path :: string()) -> boolean().
%% ====================================================================
is_group_dir(Path) ->
    case string:tokens(Path,"/") of
        [?GROUPS_BASE_DIR_NAME] -> true;
        [?GROUPS_BASE_DIR_NAME , _GroupName] ->  true;
        _ -> false
    end.