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

-include("oneprovider_modules/control_panel/cdmi.hrl").
-include("oneprovider_modules/control_panel/cdmi_capabilities.hrl").
-include("oneprovider_modules/control_panel/cdmi_container.hrl").
-include("oneprovider_modules/control_panel/cdmi_error.hrl").
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
        {ok, #fileattributes{type = ?DIR_TYPE_PROT} = Attr} -> {true, Req, State#state{attributes = Attr}};
        {ok, _} ->
            Req1 = cowboy_req:set_resp_header(<<"Location">>, utils:ensure_unicode_binary(Filepath), Req),
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
    case logical_files_manager:rmdir_recursive(Filepath) of
        ok -> {true, Req, State};
        {logical_file_system_error, Error} when Error == ?VEPERM orelse Error == ?VEACCES  -> cdmi_error:error_reply(Req, State, ?forbidden);
        Error -> cdmi_error:error_reply(Req, State, {?dir_delete_unknown_error, Error})
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
get_cdmi_container(Req, #state{opts = Opts, filepath = Filepath} = State) ->
    case logical_files_manager:check_file_perm(Filepath, read) of
        true -> ok;
        false -> throw(?forbidden)
    end,
    NewOpts = case Opts of [] -> ?default_get_dir_opts; _ -> Opts end,
    DirCdmi = prepare_container_ans(NewOpts, State#state{opts = NewOpts}),
    Response = rest_utils:encode_to_json({struct, DirCdmi}),
    {Response, Req, State}.

%% put_cdmi_container/2
%% ====================================================================
%% @doc Callback function for cdmi container PUT operation (create dir)
-spec put_cdmi_container(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
put_cdmi_container(Req, #state{filepath = Filepath, opts = Opts} = State) ->
    % parse body
    {ok, RawBody, Req1} = opn_cowboy_bridge:apply(cowboy_req, body, [Req]),
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
            {undefined, MoveURI} -> logical_files_manager:mv(utils:ensure_unicode_list(MoveURI),Filepath);
            {CopyURI, undefined} -> logical_files_manager:cp(utils:ensure_unicode_list(CopyURI),Filepath)
        end,

    %check result and update metadata
    case OperationAns of
        ok ->
            case logical_files_manager:getfileattr(Filepath) of
                {ok, Attr} ->
                    cdmi_metadata:update_user_metadata(Filepath, RequestedUserMetadata),
                    Response = rest_utils:encode_to_json(
                        {struct, prepare_container_ans(?default_get_dir_opts, State#state{attributes = Attr, opts = ?default_get_dir_opts})}),
                    Req2 = cowboy_req:set_resp_body(Response, Req1),
                    {true, Req2, State};
                Error ->
                    logical_files_manager:rmdir(Filepath),
                    cdmi_error:error_reply(Req1, State, {?get_attr_unknown_error, Error})
            end;
        {error, dir_exists} ->
            % check write permission
            case logical_files_manager:check_file_perm(Filepath, write) of
                true -> ok;
                false -> throw(?forbidden)
            end,
            URIMetadataNames = [MetadataName || {OptKey, MetadataName} <- Opts, OptKey == <<"metadata">>],
            cdmi_metadata:update_user_metadata(Filepath, RequestedUserMetadata, URIMetadataNames),
            {true, Req1, State};
        {logical_file_system_error, Error} when Error == ?VEPERM orelse Error == ?VEACCES  -> cdmi_error:error_reply(Req, State, ?forbidden);
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
        {logical_file_system_error, Error} when Error == ?VEPERM orelse Error == ?VEACCES  -> cdmi_error:error_reply(Req, State, ?forbidden);
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
    [{<<"objectName">>, utils:ensure_unicode_binary([filename:basename(Filepath), "/"])} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"parentURI">> | Tail], #state{filepath = "/"} = State) ->
    [{<<"parentURI">>,<<>>} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"parentURI">> | Tail], #state{filepath = Filepath} = State) ->
    ParentURI = utils:ensure_unicode_binary(rest_utils:ensure_path_ends_with_slash(fslogic_path:strip_path_leaf(Filepath))),
    [{<<"parentURI">>, ParentURI} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"parentID">> | Tail], #state{filepath = "/"} = State) ->
    prepare_container_ans(Tail, State);
prepare_container_ans([<<"parentID">> | Tail], #state{filepath = Filepath} = State) ->
    {ok,Uuid} = logical_files_manager:get_file_uuid(fslogic_path:strip_path_leaf(Filepath)),
    [{<<"parentID">>, cdmi_id:uuid_to_objectid(Uuid)} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"capabilitiesURI">> | Tail], State) ->
    [{<<"capabilitiesURI">>, utils:ensure_unicode_binary(?container_capability_path)} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"completionStatus">> | Tail], State) ->
    [{<<"completionStatus">>, <<"Complete">>} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"metadata">> | Tail], #state{filepath = Filepath, attributes = Attrs} = State) ->
    [{<<"metadata">>, cdmi_metadata:prepare_metadata(Filepath, Attrs)} | prepare_container_ans(Tail, State)];
prepare_container_ans([{<<"metadata">>, Prefix} | Tail], #state{filepath = Filepath, attributes = Attrs} = State) ->
    [{<<"metadata">>, cdmi_metadata:prepare_metadata(Filepath, Prefix, Attrs)} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"childrenrange">> | Tail], #state{opts = Opts, filepath = Filepath} = State) ->
    {ok, ChildNum0} = logical_files_manager:get_file_children_count(Filepath),
    ChildNum = case Filepath of "/" -> ChildNum0 + 1; _ -> ChildNum0 end,
    {From, To} = case lists:keyfind(<<"children">>, 1, Opts) of
        {<<"children">>, Begin, End} -> normalize_childrenrange(Begin, End, ChildNum);
        _ -> case ChildNum of
                 0 -> {undefined, undefined};
                 _ -> {0, ChildNum-1}
             end
    end,
    BinaryRange = case {From,To} of
                      {undefined, undefined} -> <<"">>;
                      _ -> <<(integer_to_binary(From))/binary, "-", (integer_to_binary(To))/binary>>
                  end,
    [{<<"childrenrange">>, BinaryRange} | prepare_container_ans(Tail, State)];
prepare_container_ans([{<<"children">>, From, To} | Tail], #state{filepath = Filepath} = State) ->
    {ok, ChildNum0} = logical_files_manager:get_file_children_count(Filepath),
    ChildNum = case Filepath of "/" -> ChildNum0 + 1; _ -> ChildNum0 end,
    {From1, To1} = normalize_childrenrange(From, To, ChildNum),
    {ok, List} = logical_files_manager:ls_chunked(Filepath, From1, To1),
    Childs = lists:map(
        fun(#dir_entry{name = Name, type = ?DIR_TYPE_PROT}) ->
            utils:ensure_unicode_binary(rest_utils:ensure_path_ends_with_slash(Name));
            (#dir_entry{name = Name, type = ?REG_TYPE_PROT}) ->
                utils:ensure_unicode_binary(Name)
        end, List),
    [{<<"children">>, Childs} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"children">> | Tail], #state{filepath = Filepath} = State) ->
    {ok, List} = logical_files_manager:ls_chunked(Filepath),
    Childs = lists:map(
        fun(#dir_entry{name = Name, type = ?DIR_TYPE_PROT}) ->
            utils:ensure_unicode_binary(rest_utils:ensure_path_ends_with_slash(Name));
           (#dir_entry{name = Name, type = ?REG_TYPE_PROT}) ->
               utils:ensure_unicode_binary(Name)
        end, List),
    [{<<"children">>, Childs} | prepare_container_ans(Tail, State)];
prepare_container_ans([_Other | Tail], State) ->
    prepare_container_ans(Tail, State).

%% normalize_childrenrange/1
%% ====================================================================
%% @doc Checks if given childrange is correct according to child number. Tries to correct the result
-spec normalize_childrenrange(From :: integer(), To :: integer(), ChildNum :: integer()) -> {NewFrom :: integer(), NewTo :: integer()} | no_return().
%% ====================================================================
normalize_childrenrange(From, To, _ChildNum) when From > To -> throw(?invalid_childrenrange);
normalize_childrenrange(_From, To, ChildNum) when To >= ChildNum -> throw(?invalid_childrenrange);
normalize_childrenrange(From, To, ChildNum)->
    {From, min(ChildNum - 1, To)}.

