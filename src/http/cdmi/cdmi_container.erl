%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2015-2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides function to operate on cdmi containers (directories).
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_container).
-author("Tomasz Lichon").
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("http/cdmi.hrl").
-include("global_definitions.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% API
-export([get_cdmi/2, put_cdmi/2, put_binary/2, delete_cdmi/2]).

-define(DEFAULT_GET_DIR_OPTS, [
    <<"objectType">>, <<"objectID">>, <<"objectName">>,
    <<"parentURI">>, <<"parentID">>,
    <<"capabilitiesURI">>, <<"completionStatus">>, <<"metadata">>,
    <<"childrenrange">>, <<"children">>
]).

-define(MAX_CHILDREN_PER_REQUEST, element(2, {ok, _} = application:get_env(
    ?APP_NAME, max_children_per_request
))).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Gets requested info about specified directory (container).
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {binary(), cowboy_req:req(), cdmi_handler:cdmi_req()}.
get_cdmi(Req, #cdmi_req{options = Options} = CdmiReq) ->
    NonEmptyOpts = utils:ensure_defined(Options, [], ?DEFAULT_GET_DIR_OPTS),
    DirInfo = get_directory_info(NonEmptyOpts, CdmiReq),
    {json_utils:encode(DirInfo), Req, CdmiReq}.


%%--------------------------------------------------------------------
%% @doc
%% Creates, copies or moves directory (container) if it doesn't yet
%% exist and returns new directory info.
%% Otherwise, if directory already exists, only updates it's metadata.
%% @end
%%--------------------------------------------------------------------
-spec put_cdmi(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {term(), cowboy_req:req(), cdmi_handler:cdmi_req()} | no_return().
put_cdmi(_, #cdmi_req{version = undefined}) ->
    throw(?ERROR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]));
put_cdmi(Req, #cdmi_req{
    auth = ?USER(_UserId, SessId),
    file_path = Path,
    file_attrs = Attrs,
    options = Options
} = CdmiReq) ->
    {ok, Body, Req1} = cdmi_parser:parse_body(Req),
    CopyURI = maps:get(<<"copy">>, Body, undefined),
    MoveURI = maps:get(<<"move">>, Body, undefined),
    Metadata = maps:get(<<"metadata">>, Body, undefined),

    case {Attrs, CopyURI, MoveURI} of
        % create directory
        {undefined, undefined, undefined} ->
            {ok, Guid} = ?check(lfm:mkdir(SessId, Path)),
            cdmi_metadata:update_user_metadata(SessId, {guid, Guid}, Metadata),
            prepare_create_dir_cdmi_response(Req, CdmiReq, Guid);
        % update metadata
        {#file_attr{guid = FileGuid}, undefined, undefined} ->
            cdmi_metadata:update_user_metadata(
                SessId,
                {guid, FileGuid},
                Metadata,
                [MetadataName || {<<"metadata">>, MetadataName} <- Options]
            ),
            {true, Req1, CdmiReq};
        % copy directory
        {undefined, CopyURI, undefined} ->
            {ok, Guid} = ?check(lfm:cp(
                SessId,
                {path, filepath_utils:ensure_begins_with_slash(CopyURI)},
                Path
            )),
            cdmi_metadata:update_user_metadata(SessId, {guid, Guid}, Metadata),
            prepare_create_dir_cdmi_response(Req, CdmiReq, Guid);
        % move directory
        {undefined, undefined, MoveURI} ->
            {ok, Guid} = ?check(lfm:mv(
                SessId,
                {path, filepath_utils:ensure_begins_with_slash(MoveURI)},
                Path
            )),
            cdmi_metadata:update_user_metadata(SessId, {guid, Guid}, Metadata),
            prepare_create_dir_cdmi_response(Req, CdmiReq, Guid)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates directory (container) under specified path.
%% @end
%%--------------------------------------------------------------------
-spec put_binary(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {true, cowboy_req:req(), cdmi_handler:cdmi_req()} | no_return().
put_binary(Req, #cdmi_req{auth = Auth, file_path = Path} = CdmiReq) ->
    ?check(lfm:mkdir(Auth#auth.session_id, Path)),
    {true, Req, CdmiReq}.


%%--------------------------------------------------------------------
%% @doc
%% Removes specified directory (container).
%% @end
%%--------------------------------------------------------------------
-spec delete_cdmi(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {true, cowboy_req:req(), cdmi_handler:cdmi_req()} | no_return().
delete_cdmi(Req, #cdmi_req{
    auth = ?USER(_UserId, SessionId),
    file_attrs = #file_attr{guid = Guid}
} = CdmiReq) ->
    ?check(lfm:rm_recursive(SessionId, {guid, Guid})),
    {true, Req, CdmiReq}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec prepare_create_dir_cdmi_response(cowboy_req:req(), cdmi_handler:cdmi_req(),
    file_id:file_guid()) -> {true, cowboy_req:req(), cdmi_handler:cdmi_req()}.
prepare_create_dir_cdmi_response(Req1, #cdmi_req{
    auth = ?USER(_UserId, SessionId)
} = CdmiReq, FileGuid) ->
    {ok, Attrs} = ?check(lfm:stat(SessionId, {guid, FileGuid})),
    CdmiReq2 = CdmiReq#cdmi_req{file_attrs = Attrs},
    Answer = get_directory_info(?DEFAULT_GET_DIR_OPTS, CdmiReq2),
    Req2 = cowboy_req:set_resp_body(json_utils:encode(Answer), Req1),
    {true, Req2, CdmiReq2}.


%% @private
-spec get_directory_info([RequestedInfo :: binary()], cdmi_handler:cdmi_req()) ->
    map() | no_return().
get_directory_info(RequestedInfo, #cdmi_req{
    auth = ?USER(_UserId, SessionId),
    file_path = Path,
    file_attrs = #file_attr{guid = Guid} = Attrs
}) ->
    lists:foldl(fun
        (<<"objectType">>, Acc) ->
            Acc#{<<"objectType">> => <<"application/cdmi-container">>};
        (<<"objectID">>, Acc) ->
            {ok, ObjectId} = file_id:guid_to_objectid(Guid),
            Acc#{<<"objectID">> => ObjectId};
        (<<"objectName">>, Acc) ->
            Acc#{<<"objectName">> => <<(filename:basename(Path))/binary, "/">>};
        (<<"parentURI">>, Acc) ->
            ParentURI = case Path of
                <<"/">> -> <<>>;
                _ -> filepath_utils:parent_dir(Path)
            end,
            Acc#{<<"parentURI">> => ParentURI};
        (<<"parentID">>, Acc) ->
            case Path of
                <<"/">> ->
                    Acc;
                _ ->
                    {ok, #file_attr{guid = ParentGuid}} = ?check(lfm:stat(
                        SessionId,
                        {path, filepath_utils:parent_dir(Path)}
                    )),
                    {ok, ParentObjectId} = file_id:guid_to_objectid(ParentGuid),
                    Acc#{<<"parentID">> => ParentObjectId}
            end;
        (<<"capabilitiesURI">>, Acc) ->
            Acc#{<<"capabilitiesURI">> => <<?CONTAINER_CAPABILITY_PATH>>};
        (<<"completionStatus">>, Acc) ->
            Acc#{<<"completionStatus">> => <<"Complete">>};
        (<<"metadata">>, Acc) ->
            Acc#{<<"metadata">> => cdmi_metadata:prepare_metadata(
                SessionId, {guid, Guid}, <<>>, Attrs
            )};
        ({<<"metadata">>, Prefix}, Acc) ->
            Acc#{<<"metadata">> => cdmi_metadata:prepare_metadata(
                SessionId, {guid, Guid}, Prefix, Attrs
            )};
        (<<"childrenrange">>, Acc) ->
            {ok, ChildNum} = ?check(lfm:get_children_count(SessionId, {guid, Guid})),
            {From, To} = case lists:keyfind(<<"children">>, 1, RequestedInfo) of
                {<<"children">>, Begin, End} ->
                    MaxChildren = ?MAX_CHILDREN_PER_REQUEST,
                    normalize_childrenrange(Begin, End, ChildNum, MaxChildren);
                _ ->
                    case ChildNum of
                        0 -> {undefined, undefined};
                        _ -> {0, ChildNum - 1}
                    end
            end,
            BinaryRange = case {From, To} of
                {undefined, undefined} ->
                    <<"">>;
                _ ->
                    <<
                        (integer_to_binary(From))/binary,
                        "-",
                        (integer_to_binary(To))/binary
                    >>
            end,
            Acc#{<<"childrenrange">> => BinaryRange};
        ({<<"children">>, From, To}, Acc) ->
            MaxChildren = ?MAX_CHILDREN_PER_REQUEST,
            {ok, ChildNum} = ?check(lfm:get_children_count(SessionId, {guid, Guid})),
            {From1, To1} = normalize_childrenrange(From, To, ChildNum, MaxChildren),
            {ok, List} = ?check(lfm:ls(SessionId, {guid, Guid}, From1, To1 - From1 + 1)),
            Acc#{<<"children">> => lists:map(fun({FileGuid, Name}) ->
                distinguish_directories(SessionId, FileGuid, Name)
            end, List)};
        (<<"children">>, Acc) ->
            MaxChildren = ?MAX_CHILDREN_PER_REQUEST,
            {ok, List} = ?check(lfm:ls(SessionId, {guid, Guid}, 0, MaxChildren + 1)),
            case length(List) > MaxChildren of
                true ->
                    throw(?ERROR_BAD_VALUE_TOO_HIGH(<<"childrenrange">>, MaxChildren));
                false ->
                    ok
            end,
            Acc#{<<"children">> => lists:map(fun({FileGuid, Name}) ->
                distinguish_directories(SessionId, FileGuid, Name)
            end, List)};
        (_, Acc) ->
            Acc
    end, #{}, RequestedInfo).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if given childrenrange is correct according to child number.
%% If not tries to correct the result.
%% @end
%%--------------------------------------------------------------------
-spec normalize_childrenrange(From :: integer(), To :: integer(),
    ChildNum :: integer(), MaxChildren :: integer()) ->
    {NewFrom :: integer(), NewTo :: integer()} | no_return().
normalize_childrenrange(From, To, _ChildNum, _MaxChildren) when From > To ->
    throw(?ERROR_BAD_DATA(<<"childrenrange">>));
normalize_childrenrange(_From, To, ChildNum, _MaxChildren) when To >= ChildNum ->
    throw(?ERROR_BAD_DATA(<<"childrenrange">>));
normalize_childrenrange(From, To, _ChildNum, MaxChildren) ->
    case (To - From + 1) > MaxChildren of
        true ->
            throw(?ERROR_BAD_VALUE_TOO_HIGH(<<"childrenrange">>, MaxChildren));
        false ->
            {From, To}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Distinguishes regular files from directories (for directories returns
%% path ending with slash).
%% @end
%%--------------------------------------------------------------------
-spec distinguish_directories(session:id(), file_id:file_guid(),
    Name :: binary()) -> binary() | no_return().
distinguish_directories(SessionId, Guid, Name) ->
    case lfm:stat(SessionId, {guid, Guid}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} ->
            filepath_utils:ensure_ends_with_slash(Name);
        {ok, _} ->
            Name;
        {error, Errno} ->
            throw(?ERROR_POSIX(Errno))
    end.