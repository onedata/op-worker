%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2015-2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides function to operate on cdmi containers.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_container).
-author("Tomasz Lichon").
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("http/cdmi.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

-define(DEFAULT_GET_DIR_OPTS, [
    <<"objectType">>, <<"objectID">>, <<"objectName">>,
    <<"parentURI">>, <<"parentID">>,
    <<"capabilitiesURI">>, <<"completionStatus">>, <<"metadata">>,
    <<"childrenrange">>, <<"children">>
]).

-define(run(__FunctionCall), check_result(__FunctionCall)).

%% API
-export([get_cdmi/2, put_cdmi/2, put_binary/2, delete_cdmi/2]).


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
    Answer = get_directory_info(NonEmptyOpts, CdmiReq),
    {json_utils:encode(Answer), Req, CdmiReq}.


%%--------------------------------------------------------------------
%% @doc
%% Creates, copies or moves directory (container) if it doesn't already
%% exist or updates it's metadata if it does exist.
%% @end
%%--------------------------------------------------------------------
-spec put_cdmi(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {term(), cowboy_req:req(), cdmi_handler:cdmi_req()} | no_return().
put_cdmi(_, #cdmi_req{version = undefined}) ->
    throw(?ERROR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]));
put_cdmi(Req, #cdmi_req{
    client = ?USER(_UserId, SessionId),
    file_path = Path,
    file_attrs = Attrs,
    options = Options
} = CdmiReq) ->
    {ok, Body, Req1} = cdmi_arg_parser:parse_body(Req),
    RequestedCopyURI = maps:get(<<"copy">>, Body, undefined),
    RequestedMoveURI = maps:get(<<"move">>, Body, undefined),
    RequestedUserMetadata = maps:get(<<"metadata">>, Body, undefined),

    % create dir using mkdir/cp/mv
    {ok, OperationPerformed, Guid} =
        case {Attrs, RequestedCopyURI, RequestedMoveURI} of
            {undefined, undefined, undefined} ->
                {ok, NewGuid} = ?run(lfm:mkdir(SessionId, Path)),
                {ok, created, NewGuid};
            {#file_attr{guid = NewGuid}, undefined, undefined} ->
                {ok, none, NewGuid};
            {undefined, CopyURI, undefined} ->
                {ok, NewGuid} = ?run(lfm:cp(
                    SessionId,
                    {path, filepath_utils:ensure_begins_with_slash(CopyURI)},
                    Path
                )),
                {ok, copied, NewGuid};
            {undefined, undefined, MoveURI} ->
                {ok, NewGuid} = ?run(lfm:mv(
                    SessionId,
                    {path, filepath_utils:ensure_begins_with_slash(MoveURI)},
                    Path
                )),
                {ok, moved, NewGuid}
        end,

    % update metadata and return result
    case OperationPerformed of
        none ->
            URIMetadataNames = [MetadataName || {OptKey, MetadataName} <- Options, OptKey == <<"metadata">>],
            ok = cdmi_metadata:update_user_metadata(
                SessionId,
                {guid, Guid},
                RequestedUserMetadata,
                URIMetadataNames
            ),
            {true, Req1, CdmiReq};
        _ ->
            {ok, NewAttrs} = ?run(lfm:stat(SessionId, {guid, Guid})),
            CdmiReq2 = CdmiReq#cdmi_req{file_attrs = NewAttrs},
            ok = cdmi_metadata:update_user_metadata(
                SessionId,
                {guid, Guid},
                RequestedUserMetadata
            ),
            Answer = get_directory_info(?DEFAULT_GET_DIR_OPTS, CdmiReq2),
            Req2 = cowboy_req:set_resp_body(json_utils:encode(Answer), Req1),
            {true, Req2, CdmiReq}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates directory (container) under specified path.
%% @end
%%--------------------------------------------------------------------
-spec put_binary(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {true, cowboy_req:req(), cdmi_handler:cdmi_req()} | no_return().
put_binary(Req, #cdmi_req{client = Client, file_path = Path} = CdmiReq) ->
    case lfm:mkdir(Client#client.session_id, Path) of
        {ok, _} ->
            {true, Req, CdmiReq};
        {error, Errno} ->
            throw(?ERROR_POSIX(Errno))
    end.


%%--------------------------------------------------------------------
%% @doc
%% Removes specified directory (container).
%% @end
%%--------------------------------------------------------------------
-spec delete_cdmi(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {true, cowboy_req:req(), cdmi_handler:cdmi_req()} | no_return().
delete_cdmi(Req, #cdmi_req{
    client = ?USER(_UserId, SessionId),
    file_attrs = #file_attr{guid = Guid}
} = CdmiReq) ->
    case lfm:rm_recursive(SessionId, {guid, Guid}) of
        ok ->
            {true, Req, CdmiReq};
        {error, Errno} ->
            throw(?ERROR_POSIX(Errno))
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_directory_info([RequestedInfo :: binary()], cdmi_handler:cdmi_req()) ->
    map() | no_return().
get_directory_info(RequestedInfo, #cdmi_req{
    client = ?USER(_UserId, SessionId),
    file_path = Path,
    file_attrs = #file_attr{guid = Guid} = Attrs
}) ->
    lists:foldl(
        fun
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
                        case lfm:stat(SessionId, {path, filepath_utils:parent_dir(Path)}) of
                            {ok, #file_attr{guid = ParentGuid}} ->
                                {ok, ObjectId} = file_id:guid_to_objectid(ParentGuid),
                                Acc#{<<"parentID">> => ObjectId};
                            {error, Errno} ->
                                throw(?ERROR_POSIX(Errno))
                        end
                end;
            (<<"capabilitiesURI">>, Acc) ->
                Acc#{<<"capabilitiesURI">> => ?CONTAINER_CAPABILITY_PATH};
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
                {ok, ChildNum} = ?run(lfm:get_children_count(SessionId, {guid, Guid})),
                {From, To} = case lists:keyfind(<<"children">>, 1, RequestedInfo) of
                    {<<"children">>, Begin, End} ->
                        {ok, MaxChildren} = application:get_env(?APP_NAME, max_children_per_request),
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
                        <<(integer_to_binary(From))/binary, "-", (integer_to_binary(To))/binary>>
                end,
                Acc#{<<"childrenrange">> => BinaryRange};
            ({<<"children">>, From, To}, Acc) ->
                {ok, MaxChildren} = application:get_env(?APP_NAME, max_children_per_request),
                {ok, ChildNum} = ?run(lfm:get_children_count(SessionId, {guid, Guid})),
                {From1, To1} = normalize_childrenrange(From, To, ChildNum, MaxChildren),
                {ok, List} = ?run(lfm:ls(SessionId, {guid, Guid}, From1, To1 - From1 + 1)),
                Acc#{<<"children">> => lists:map(fun({FileGuid, Name}) ->
                    distinguish_files(FileGuid, Name, SessionId)
                end, List)};
            (<<"children">>, Acc) ->
                {ok, MaxChildren} = application:get_env(?APP_NAME, max_children_per_request),
                {ok, List} = ?run(lfm:ls(SessionId, {guid, Guid}, 0, MaxChildren + 1)),
                terminate_if_too_many_children(List, MaxChildren),
                Acc#{<<"children">> => lists:map(fun({FileGuid, Name}) ->
                    distinguish_files(FileGuid, Name, SessionId)
                end, List)};
            (_, Acc) ->
                Acc
        end,
        #{},
        RequestedInfo
    ).


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
normalize_childrenrange(From, To, ChildNum, MaxChildren) ->
    To2 = min(ChildNum - 1, To),
    case MaxChildren < (To2 - From + 1) of
        true ->
            throw(?ERROR_BAD_VALUE_TOO_HIGH(<<"childrenrange">>, MaxChildren));
        false ->
            {From, To2}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Distinguishes regular files from directories
%% (for regular files returns path ending with slash)
%% @end
%%--------------------------------------------------------------------
-spec distinguish_files(file_id:file_guid(), Name :: binary(), session:id()) ->
    binary() | no_return().
distinguish_files(Guid, Name, Auth) ->
    case lfm:stat(Auth, {guid, Guid}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} ->
            filepath_utils:ensure_ends_with_slash(Name);
        {ok, _} ->
            Name;
        {error, Errno} ->
            throw(?ERROR_POSIX(Errno))
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Terminates request with error if requested childrenrange exceeds system limit.
%% @end
%%--------------------------------------------------------------------
-spec terminate_if_too_many_children(list(), non_neg_integer()) -> ok | no_return().
terminate_if_too_many_children(List, MaxChildren) when length(List) > MaxChildren ->
    throw(?ERROR_BAD_VALUE_TOO_HIGH(<<"childrenrange">>, MaxChildren));
terminate_if_too_many_children(_, _) ->
    ok.


%% @private
-spec check_result({ok, term()} | {error, term()}) ->
    {ok, term()} | no_return().
check_result({ok, _} = Res) -> Res;
check_result({error, Errno}) -> throw(?ERROR_POSIX(Errno)).
