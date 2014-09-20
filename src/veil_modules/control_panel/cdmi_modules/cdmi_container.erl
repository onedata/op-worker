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
-include("veil_modules/control_panel/cdmi_error.hrl").
-include("files_common.hrl").

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
malformed_request(Req, #state{cdmi_version = Version, method = <<"PUT">>, filepath = Filepath} = State) when is_binary(Version) ->
    case cowboy_req:header(<<"content-type">>, Req) of
        {<<"application/cdmi-container">>, Req1} -> {false, Req1, State#state{filepath = fslogic_path:get_short_file_name(Filepath)}};
        _ -> cdmi_error:error_reply(Req, State, ?invalid_content_type)
    end;
malformed_request(Req, #state{filepath = Filepath} = State) ->
    {false, Req, State#state{filepath = fslogic_path:get_short_file_name(Filepath)}}.

%% resource_exists/2
%% ====================================================================
%% @doc Determines if resource, that can be obtained from state, exists.
-spec resource_exists(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
resource_exists(Req, State = #state{filepath = Filepath}) ->
    case logical_files_manager:getfileattr(Filepath) of
        {ok, #fileattributes{type = "DIR"} = Attr} -> {true, Req, State#state{attributes = Attr}};
        {ok, _} ->
            Req1 = cowboy_req:set_resp_header(<<"Location">>, list_to_binary(Filepath), Req),
            cdmi_error:error_reply(Req1,State,{?moved_permanently, Filepath});
        _ -> {false, Req, State}
    end.

%% content_types_provided/2
%% ====================================================================
%% @doc
%% Returns content types that can be provided and what functions should be used to process the request.
%% Before adding new content type make sure that adequate routing function
%% exists in cdmi_handler
%% @end
%% ====================================================================
-spec content_types_provided(req(), #state{}) -> {[{ContentType, Method}], req(), #state{}} when
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
%% ====================================================================
-spec content_types_accepted(req(), #state{}) -> {[{ContentType, Method}], req(), #state{}} when
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
            case fs_remove_dir(Filepath) of
                ok -> {true, Req, State};
                Error -> cdmi_error:error_reply(Req, State, {?dir_delete_unknown_error, Error}) %todo handle dir error forbidden
            end;
        true -> cdmi_error:error_reply(Req, State, ?group_dir_delete)
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
put_cdmi_container(Req, #state{filepath = Filepath, opts = Opts} = State) ->
    % parse body
    {ok, RawBody, Req1} = veil_cowboy_bridge:apply(cowboy_req, body, [Req]),
    Body = rest_utils:parse_body(RawBody),
    ok = rest_utils:validate_body(Body),

    % prepare body fields
    RequestedUserMetadata = proplists:get_value(<<"metadata">>, Body),
    RequestedCopyURI = proplists:get_value(<<"copy">>, Body),
    RequestedMoveURI = proplists:get_value(<<"move">>, Body),

    % make sure dir is created
    OperationAns =
        case {RequestedCopyURI, RequestedMoveURI} of
            {undefined, undefined} -> logical_files_manager:mkdir(Filepath);
            {undefined, MoveURI} -> logical_files_manager:mv(binary_to_list(MoveURI),Filepath);
            {_CopyURI, undefined} -> unimplemented
        end,

    %check result and update metadata
    case OperationAns of
        ok ->
            case logical_files_manager:getfileattr(Filepath) of
                {ok, Attr} ->
                    cdmi_metadata:update_user_metadata(Filepath, RequestedUserMetadata),
                    Response = rest_utils:encode_to_json(
                        {struct, prepare_container_ans(?default_get_dir_opts, State#state{attributes = Attr})}),
                    Req2 = cowboy_req:set_resp_body(Response, Req1),
                    {true, Req2, State};
                Error -> %todo handle getattr forbidden
                    logical_files_manager:rmdir(Filepath),
                    cdmi_error:error_reply(Req1, State, {?get_attr_unknown_error, Error})
            end;
        {error, dir_exists} ->
            URIMetadataNames = [MetadataName || {OptKey, MetadataName} <- Opts, OptKey == <<"metadata">>],
            cdmi_metadata:update_user_metadata(Filepath, RequestedUserMetadata, URIMetadataNames),
            {true, Req1, State};
        {logical_file_system_error, ?VEPERM} -> cdmi_error:error_reply(Req, State, ?forbidden);
        {logical_file_system_error, ?VEACCES} -> cdmi_error:error_reply(Req, State, ?forbidden);
        {logical_file_system_error, ?VENOENT} -> cdmi_error:error_reply(Req1, State, ?parent_not_found);
        Error -> cdmi_error:error_reply(Req1, State, {?put_container_unknown_error, Error})
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
        {error, dir_exists} -> cdmi_error:error_reply(Req, State, ?put_container_conflict);
        {logical_file_system_error, ?VENOENT} -> cdmi_error:error_reply(Req, State, ?parent_not_found);
        {logical_file_system_error, ?VEPERM} -> cdmi_error:error_reply(Req, State, ?forbidden);
        {logical_file_system_error, ?VEACCES} -> cdmi_error:error_reply(Req, State, ?forbidden);
        Error -> cdmi_error:error_reply(Req, State, {?put_container_unknown_error, Error})
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
    {ok, Uuid} = logical_files_manager:get_file_uuid(Filepath),
    [{<<"objectID">>, cdmi_id:uuid_to_objectid(Uuid)} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"objectName">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"objectName">>, list_to_binary([filename:basename(Filepath), "/"])} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"parentURI">> | Tail], #state{filepath = "/"} = State) ->
    [{<<"parentURI">>,<<>>} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"parentURI">> | Tail], #state{filepath = Filepath} = State) ->
    ParentURI = list_to_binary(rest_utils:ensure_path_ends_with_slash(fslogic_path:strip_path_leaf(Filepath))),
    [{<<"parentURI">>, ParentURI} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"parentID">> | Tail], #state{filepath = "/"} = State) ->
    prepare_container_ans(Tail, State);
prepare_container_ans([<<"parentID">> | Tail], #state{filepath = Filepath} = State) ->
    {ok,Uuid} = logical_files_manager:get_file_uuid(fslogic_path:strip_path_leaf(Filepath)),
    [{<<"parentID">>, cdmi_id:uuid_to_objectid(Uuid)} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"capabilitiesURI">> | Tail], State) ->
    [{<<"capabilitiesURI">>, list_to_binary(?container_capability_path)} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"completionStatus">> | Tail], State) ->
    [{<<"completionStatus">>, <<"Complete">>} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"metadata">> | Tail], #state{filepath = Filepath, attributes = Attrs} = State) ->
    [{<<"metadata">>, cdmi_metadata:prepare_metadata(Filepath, Attrs)} | prepare_container_ans(Tail, State)];
prepare_container_ans([{<<"metadata">>, Prefix} | Tail], #state{filepath = Filepath, attributes = Attrs} = State) ->
    [{<<"metadata">>, cdmi_metadata:prepare_metadata(Filepath, Prefix, Attrs)} | prepare_container_ans(Tail, State)];
prepare_container_ans([{<<"children">>, From, To} | Tail], #state{filepath = Filepath} = State) ->
    Childs = lists:map(
        fun(#dir_entry{name = Name, type = ?DIR_TYPE_PROT}) ->
            list_to_binary(rest_utils:ensure_path_ends_with_slash(Name));
            (#dir_entry{name = Name, type = ?REG_TYPE_PROT}) ->
                list_to_binary(Name)
        end,
        rest_utils:list_dir(Filepath, From, To)),
    [{<<"children">>, Childs} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"children">> | Tail], #state{filepath = Filepath} = State) ->
    Childs = lists:map(
        fun(#dir_entry{name = Name, type = ?DIR_TYPE_PROT}) ->
                list_to_binary(rest_utils:ensure_path_ends_with_slash(Name));
           (#dir_entry{name = Name, type = ?REG_TYPE_PROT}) ->
                list_to_binary(Name)
        end,
        rest_utils:list_dir(Filepath)),
    [{<<"children">>, Childs} | prepare_container_ans(Tail, State)];
prepare_container_ans([_Other | Tail], State) ->
    prepare_container_ans(Tail, State).


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
            lists:foreach(
                fun(#dir_entry{name = Name, type = ?REG_TYPE_PROT}) -> logical_files_manager:delete(filename:join(DirPath, Name));
                   (#dir_entry{name = Name, type = ?DIR_TYPE_PROT}) -> fs_remove_dir(filename:join(DirPath, Name))
                end,
                rest_utils:list_dir(DirPath)),
            logical_files_manager:rmdir(DirPath)
    end.

%% is_group_dir/1
%% ====================================================================
%% @doc Returns true when Path points to group directory (or groups root directory)
-spec is_group_dir(Path :: string()) -> boolean().
%% ====================================================================
is_group_dir(Path) ->
    case string:tokens(Path,"/") of
        [] -> true;
        [?SPACES_BASE_DIR_NAME] -> true;
        [?SPACES_BASE_DIR_NAME , _GroupName] ->  true;
        _ -> false
    end.