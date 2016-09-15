%%%--------------------------------------------------------------------
%%% @author Piotr Ociepka
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module implements some functions for parsing
%%% and processing parameters of request.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_arg_parser).
-author("Piotr Ociepka").

-include("http/http_common.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").
-include("http/rest/cdmi/cdmi_capabilities.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/logging.hrl").

% keys that are forbidden to appear simultaneously in a request's body
-define(KEYS_REQUIRED_TO_BE_EXCLUSIVE, [<<"deserialize">>, <<"copy">>,
    <<"move">>, <<"reference">>, <<"deserializevalue">>, <<"value">>]).

-define(CDMI_VERSION_HEADER, <<"x-cdmi-specification-version">>).

%% API
-export([malformed_request/2, malformed_capability_request/2,
    malformed_objectid_request/2, get_ranges/2, parse_body/1,
    parse_byte_range/2]).

%% Test API
-export([get_supported_version/1, parse_content/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Extract the CDMI version and options and put it in State.
%%--------------------------------------------------------------------
-spec malformed_request(req(), maps:map()) -> {false, req(), maps:map()}.
malformed_request(Req, State) ->
    {State2, Req2} = add_version_to_state(Req, State),
    {State3, Req3} = add_opts_to_state(Req2, State2),
    {State4, Req4} = add_path_to_state(Req3, State3),
    {false, Req4, State4}.

%%--------------------------------------------------------------------
%% @doc
%% Extract the CDMI version and options and put it in State. Throw when
%% version is not supportet
%% @end
%%--------------------------------------------------------------------
-spec malformed_capability_request(req(), maps:map()) -> {boolean(), req(), maps:map()} | no_return().
malformed_capability_request(Req, State) ->
    {false, Req, State2} = cdmi_arg_parser:malformed_request(Req, State),
    case maps:find(cdmi_version, State2) of
        {ok, _} -> {false, Req, State2};
        _ -> throw(?ERROR_UNSUPPORTED_VERSION)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Check format of objectid request. Prepare version, options and path.
%% Add them to request state and change handler to object/container/capability
%% @end
%%--------------------------------------------------------------------
-spec malformed_objectid_request(req(), maps:map()) -> {false, req(), maps:map()} | no_return().
malformed_objectid_request(Req, State) ->
    {State2 = #{path := Path}, Req2} = add_objectid_path_to_state(Req, State),
    {State3, Req3} = add_version_to_state(Req2, State2),
    {State4, Req4} = add_opts_to_state(Req3, State3),

    {Handler, Req5} = choose_handler(Req4, Path),
    onedata_handler_management_api:set_handler(Handler),
    {false, Req5, State4}.

%%--------------------------------------------------------------------
%% @doc Get requested ranges list.
%%--------------------------------------------------------------------
-spec get_ranges(Req :: req(), Size :: non_neg_integer()) ->
    {[{non_neg_integer(), non_neg_integer()}] | undefined, req()}.
get_ranges(Req, Size) ->
    {RawRange, Req1} = cowboy_req:header(<<"range">>, Req),
    case RawRange of
        undefined -> {undefined, Req1};
        _ ->
            case parse_byte_range(RawRange, Size) of
                invalid -> throw(?ERROR_INVALID_RANGE);
                Ranges -> {Ranges, Req1}
            end
    end.

%%--------------------------------------------------------------------
%% @doc Reads whole body and decodes it as json.
%%--------------------------------------------------------------------
-spec parse_body(cowboy_req:req()) -> {ok, maps:map(), cowboy_req:req()}.
parse_body(Req) ->
    {ok, RawBody, Req1} = cowboy_req:body(Req),
    Body = json_utils:decode_map(RawBody),
    ok = validate_body(Body),
    {ok, Body, Req1}.

%%--------------------------------------------------------------------
%% @doc Parses byte ranges from 'Range' http header format to list of
%% erlang range tuples, i. e. <<"1-5,-3">> for a file with length 10
%% will produce -> [{1,5},{7,9}]
%% @end
%%--------------------------------------------------------------------
-spec parse_byte_range(binary() | list(), non_neg_integer()) -> list(Range) | invalid when
    Range :: {From :: integer(), To :: integer()} | invalid.
parse_byte_range(Range, Size) when is_binary(Range) ->
    Ranges = parse_byte_range(binary:split(Range, <<",">>, [global]), Size),
    case lists:member(invalid, Ranges) of
        true -> invalid;
        false -> Ranges
    end;
parse_byte_range([], _) ->
    [];
parse_byte_range([First | Rest], Size) ->
    Range =
        case binary:split(First, <<"-">>, [global]) of
            [<<>>, FromEnd] ->
                {max(0, Size - binary_to_integer(FromEnd)), Size - 1};
            [From, <<>>] -> {binary_to_integer(From), Size - 1};
            [From_, To] ->
                {binary_to_integer(From_), binary_to_integer(To)};
            _ -> invalid
        end,
    case Range of
        invalid -> [invalid];
        {Begin, End} when Begin > End -> [invalid];
        ValidRange -> [ValidRange | parse_byte_range(Rest, Size)]
    end.

%%--------------------------------------------------------------------
%% @doc
%% Parses content-type header to mimetype and charset part, if charset
%% is other than utf-8, function returns undefined
%% @end
%%--------------------------------------------------------------------
-spec parse_content(binary()) -> {Mimetype :: binary(), Encoding :: binary() | undefined}.
parse_content(Content) ->
    case binary:split(Content, <<";">>) of
        [RawMimetype, RawEncoding] ->
            case binary:split(utils:trim_spaces(RawEncoding), <<"=">>) of
                [<<"charset">>, <<"utf-8">>] ->
                    {utils:trim_spaces(RawMimetype), <<"utf-8">>};
                _ ->
                    {utils:trim_spaces(RawMimetype), undefined}
            end;
        [RawMimetype] ->
            {utils:trim_spaces(RawMimetype), undefined}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-type result_state() :: #{options => list(), cdmi_version => binary(),
path => onedata_file_api:file_path()}.

%%--------------------------------------------------------------------
%% @doc
%% Parses request's version adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec add_version_to_state(cowboy_req:req(), maps:map()) ->
    {result_state(), cowboy_req:req()}.
add_version_to_state(Req, State) ->
    {RawVersion, NewReq} = cowboy_req:header(?CDMI_VERSION_HEADER, Req),
    Version = get_supported_version(RawVersion),
    {State#{cdmi_version => Version}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Parses request's query string options and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec add_opts_to_state(cowboy_req:req(), maps:map()) ->
    {result_state(), cowboy_req:req()}.
add_opts_to_state(Req, State) ->
    {Qs, NewReq} = cowboy_req:qs(Req),
    Opts = parse_opts(Qs),
    {State#{options => Opts}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves file path from req and adds it to state.
%% @end
%%--------------------------------------------------------------------
-spec add_path_to_state(cowboy_req:req(), maps:map()) ->
    {result_state(), cowboy_req:req()}.
add_path_to_state(Req, State) ->
    {RawPath, NewReq} = cowboy_req:path(Req),
    <<"/cdmi", Path/binary>> = RawPath,
    {State#{path => Path}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Expand resource path from objectid format to full, absolute filepath.
%% Such full path is added to State.
%%
%% Converts {ObjectId}{Path} -> {BasePath}{Path}
%% e. g.
%% {IdOfFile1} -> /file1
%% {IdOfSpacesDir}/space1 -> spaces/space1
%% {IdOfRootDir} -> /
%% @end
%%--------------------------------------------------------------------
-spec add_objectid_path_to_state(cowboy_req:req(), maps:map()) ->
    {result_state(), cowboy_req:req()}.
add_objectid_path_to_state(Req, State) ->
    % get objectid
    {Id, Req2} = cowboy_req:binding(id, Req),
    {RawPath, Req3} = cowboy_req:path(Req2),
    IdSize = byte_size(Id),
    <<"/cdmi/cdmi_objectid/", Id:IdSize/binary, Path/binary>> = RawPath,

    % get uuid from objectid
    Uuid =
        case cdmi_id:objectid_to_uuid(Id) of
            {ok, Uuid_} -> Uuid_;
            _ -> throw(?ERROR_INVALID_OBJECTID)
        end,

    % get path of object with that uuid
    {BasePath, Req4} =
        case is_capability_object(Req3) of
            {true, Req3_1} ->
                {proplists:get_value(Id, ?CapabilityPathById), Req3_1};
            {false, Req3_1} ->
                {Auth, Req3_2} = try_authenticate(Req3_1),
                {ok, NewPath} = onedata_file_api:get_file_path(Auth, Uuid),
                {NewPath, Req3_2}
        end,

    % concatenate BasePath and Path to FullPath
    FullPath =
        case BasePath of
            <<"/">> -> Path;
            _ -> <<BasePath/binary, Path/binary>>
        end,

    {State#{path => FullPath}, Req4}.

%%--------------------------------------------------------------------
%% @doc Extract the CDMI version from request arguments string.
%%--------------------------------------------------------------------
-spec get_supported_version(list() | binary()) ->
    binary() | undefined.
get_supported_version(undefined) -> undefined;
get_supported_version(VersionBinary) when is_binary(VersionBinary) ->
    VersionList = lists:map(fun utils:trim_spaces/1, binary:split(VersionBinary, <<",">>, [global])),
    get_supported_version(VersionList);
get_supported_version([]) -> throw(?ERROR_UNSUPPORTED_VERSION);
get_supported_version([<<"1.1.1">> | _Rest]) -> <<"1.1.1">>;
get_supported_version([<<"1.1">> | _Rest]) -> <<"1.1.1">>;
get_supported_version([_Version | Rest]) -> get_supported_version(Rest).


%%--------------------------------------------------------------------
%% @doc Parses given cowboy 'qs' opts (all that appears after '?' in url), splitting
%% them by ';' separator and handling simple and range values,
%% i. e. input: binary("aaa;bbb:1-2;ccc;ddd:fff") will return
%% [binary(aaa),{binary(bbb),1,2},binary(ccc),{binary(ddd),binary(fff)}]
%% @end
%%--------------------------------------------------------------------
-spec parse_opts(binary()) -> [binary() | {binary(), binary()} | {binary(), From :: integer(), To :: integer()}].
parse_opts(<<>>) ->
    [];
parse_opts(RawOpts) ->
    Opts = binary:split(RawOpts, <<";">>, [global]),
    lists:map(
        fun
            (Opt) when is_binary(Opt) ->
                case binary:split(Opt, <<":">>) of
                    [SimpleOpt] -> SimpleOpt;
                    [SimpleOpt, Range] ->
                        case binary:split(Range, <<"-">>) of
                            [SimpleVal] -> {SimpleOpt, SimpleVal};
                            [From, To] ->
                                {SimpleOpt, binary_to_integer(From), binary_to_integer(To)}
                        end
                end;
            (_Other) ->
                throw(?ERROR_MALFORMED_QS)
        end,
        Opts
    ).

%%--------------------------------------------------------------------
%% @doc Validates correctness of request's body.
%%--------------------------------------------------------------------
-spec validate_body(maps:map()) -> ok | no_return().
validate_body(Body) ->
    Keys = maps:keys(Body),
    KeySet = sets:from_list(Keys),
    ExclusiveRequiredKeysSet = sets:from_list(?KEYS_REQUIRED_TO_BE_EXCLUSIVE),
    case sets:size(sets:intersection(KeySet, ExclusiveRequiredKeysSet)) of
        N when N > 1 -> throw(?ERROR_CONFLICTING_BODY_FIELDS);
        _ -> ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Chooses adequate handler for objectid request, on basis of filepath.
%% @end
%%--------------------------------------------------------------------
-spec choose_handler(cowboy_req:req(), onedata_file_api:file_path()) ->
    {module(), cowboy_req:req()}.
choose_handler(Req, Path) ->
    case filepath_utils:ends_with_slash(Path) of
        true ->
            case is_capability_object(Req) of
                {false, Req2} ->
                    {cdmi_container_handler, Req2};
                {true, Req2} ->
                    <<"/", RelativePath/binary>> = Path,
                    case RelativePath of
                        ?root_capability_path ->
                            {cdmi_capabilities_handler, Req2};
                        ?dataobject_capability_path ->
                            {cdmi_dataobject_capabilities_handler, Req2};
                        ?container_capability_path ->
                            {cdmi_container_capabilities_handler, Req2}
                    end
            end;
        false ->
            {cdmi_object_handler, Req}
    end.

%%--------------------------------------------------------------------
%% @doc Checks if this objectid request points to capability object
%%--------------------------------------------------------------------
-spec is_capability_object(req()) -> {boolean(), cowboy_req:req()}.
is_capability_object(Req) ->
    {Path, NewReq} = cowboy_req:path(Req),
    Answer =
        case binary:split(Path, <<"/">>, [global]) of
            [<<"">>, <<"cdmi">>, <<"cdmi_objectid">>, Id | _Rest] ->
                proplists:is_defined(Id, ?CapabilityPathById);
            _ -> false
        end,
    {Answer, NewReq}.

%%--------------------------------------------------------------------
%% @doc Authenticate user or throw ERROR_UNAUTHORIZED in case of error
%%--------------------------------------------------------------------
-spec try_authenticate(cowboy_req:req()) -> {onedata_auth_api:auth(), cowboy_req:req()}.
try_authenticate(Req) ->
    case onedata_auth_api:authenticate(Req) of
        {{ok, Auth}, Req2} ->
            {Auth, Req2};
        _ ->
            throw(?ERROR_UNAUTHORIZED)
    end.