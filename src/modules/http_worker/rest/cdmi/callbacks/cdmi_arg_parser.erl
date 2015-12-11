%%%--------------------------------------------------------------------
%%% @author Piotr Ociepka
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

-include("modules/http_worker/http_common.hrl").
-include("modules/http_worker/rest/cdmi/cdmi_errors.hrl").
-include("modules/http_worker/rest/cdmi/cdmi_capabilities.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/logging.hrl").

% exclusive body fields
-define(KEYS_REQUIRED_TO_BE_EXCLUSIVE, [<<"deserialize">>, <<"copy">>,
    <<"move">>, <<"reference">>, <<"deserializevalue">>, <<"value">>]).

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
-spec malformed_request(req(), #{}) -> {false, req(), #{}}.
malformed_request(Req, State) ->
    {RawVersion, Req2} = cowboy_req:header(<<"x-cdmi-specification-version">>, Req),
    Version = get_supported_version(RawVersion),
    {Qs, Req3} = cowboy_req:qs(Req2),
    Opts = parse_opts(Qs),
    {RawPath, Req4} = cowboy_req:path(Req3),
    <<"/cdmi", Path/binary>> = RawPath,

    NewState = State#{cdmi_version => Version, options => Opts, path => Path},
    {false, Req4, NewState}.

%%--------------------------------------------------------------------
%% @doc
%% Extract the CDMI version and options and put it in State. Throw when
%% version is not supportet
%% @end
%%--------------------------------------------------------------------
-spec malformed_capability_request(req(), #{}) -> {boolean(), req(), #{}} | no_return().
malformed_capability_request(Req, State) ->
    {false, Req, State2} = cdmi_arg_parser:malformed_request(Req, State),
    case maps:find(cdmi_version, State2) of
        {ok, _} -> {false, Req, State2};
        _ -> throw(?unsupported_version)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Check format of objectid request. Prepare version, options and path.
%% Add them to request state and change handler to object/container/capability
%% @end
%%--------------------------------------------------------------------
-spec malformed_objectid_request(req(), #{}) -> {false, req(), #{}} | no_return().
malformed_objectid_request(Req, State) ->
    % get objectid
    {Id, Req1} = cowboy_req:binding(id, Req),
    {RawVersion, Req2} = cowboy_req:header(<<"x-cdmi-specification-version">>, Req1),
    Version = get_supported_version(RawVersion),
    {Qs, Req3} = cowboy_req:qs(Req2),
    Opts = parse_opts(Qs),
    {RawPath, Req4} = cowboy_req:path(Req3),
    {Id, Req5} = cowboy_req:binding(id, Req4),
    IdSize = byte_size(Id),
    <<"/cdmi/cdmi_objectid/", Id:IdSize/binary, Path/binary>> = RawPath,

    % get uuid from objectid
    Uuid =
        case cdmi_id:objectid_to_uuid(Id) of
            {ok, Uuid_} -> Uuid_;
            _ -> throw(?invalid_objectid)
        end,

    % get path of object with that uuid
    {BasePath, Req6} =
        case is_capability_object(Req5) of
            true -> {proplists:get_value(Id, ?CapabilityPathById), Req5};
            false ->
                {{ok, Auth}, Req6_} = onedata_auth_api:authenticate(Req5), %todo check for no permission
                {ok, NewPath} = onedata_file_api:get_file_path(Auth, Uuid),
                {NewPath, Req6_}
        end,

    % join base name with the rest of filepath
    FullPath =
        case BasePath of
            <<"/">> -> Path;
            _ -> <<BasePath/binary, Path/binary>>
        end,
    {RawVersion, Req7} = cowboy_req:header(<<"x-cdmi-specification-version">>, Req6),
    Version = get_supported_version(RawVersion),
    {Qs, Req8} = cowboy_req:qs(Req7),
    Opts = parse_opts(Qs),

    % delegate request to cdmi_container/cdmi_object/cdmi_capability (depending on filepath)
    case Path =/= <<"">> andalso binary:last(Path) =:= $/ of
        true ->
            case is_capability_object(Req8) of %todo update req
                false ->
                    onedata_handler_management_api:set_handler(cdmi_container_handler),
                    NewState = State#{cdmi_version => Version, options => Opts, path => FullPath},
                    {false, Req8, NewState};
                true ->
                    case BasePath of
                        ?root_capability_path ->
                            onedata_handler_management_api:set_handler(cdmi_capabilities_handler),
                            NewState = State#{cdmi_version => Version, options => Opts, path => FullPath},
                            {false, Req8, NewState};
                        ?dataobject_capability_path ->
                            onedata_handler_management_api:set_handler(cdmi_dataobject_capabilities_handler),
                            NewState = State#{cdmi_version => Version, options => Opts, path => FullPath},
                            {false, Req8, NewState};
                        ?container_capability_path ->
                            onedata_handler_management_api:set_handler(cdmi_container_capabilities_handler),
                            NewState = State#{cdmi_version => Version, options => Opts, path => FullPath},
                            {false, Req8, NewState}
                    end
            end;
        false ->
            onedata_handler_management_api:set_handler(cdmi_object_handler),
            NewState = State#{cdmi_version => Version, options => Opts, path => FullPath},
            {false, Req8, NewState}
    end.

%%--------------------------------------------------------------------
%% @doc Get requested ranges list.
%%--------------------------------------------------------------------
-spec get_ranges(Req :: req(), #{}) ->
    {[{non_neg_integer(), non_neg_integer()}] | undefined, req()}.
get_ranges(Req, State) ->
    {RawRange, Req1} = cowboy_req:header(<<"range">>, Req),
    case RawRange of
        undefined -> {undefined, Req1};
        _ ->
            case parse_byte_range(State, RawRange) of
                invalid ->throw(?invalid_range);
                Ranges -> {Ranges, Req1}
            end
    end.

%%--------------------------------------------------------------------
%% @doc Reads whole body and decodes it as json.
%%--------------------------------------------------------------------
-spec parse_body(cowboy_req:req()) -> {ok, list(), cowboy_req:req()}.
parse_body(Req) ->
    {ok, RawBody, Req1} = cowboy_req:body(Req),
    Body = json_utils:decode(RawBody),
    ok = validate_body(Body),
    {ok, Body, Req1}.

%%--------------------------------------------------------------------
%% @doc Parses byte ranges from 'Range' http header format to list of
%% erlang range tuples, i. e. <<"1-5,-3">> for a file with length 10
%% will produce -> [{1,5},{7,9}]
%% @end
%%--------------------------------------------------------------------
-spec parse_byte_range(#{}, binary() | list()) -> list(Range) | invalid when
    Range :: {From :: integer(), To :: integer()} | invalid.
parse_byte_range(State, Range) when is_binary(Range) ->
    Ranges = parse_byte_range(State, binary:split(Range, <<",">>, [global])),
    case lists:member(invalid, Ranges) of
        true -> invalid;
        false -> Ranges
    end;
parse_byte_range(_, []) ->
    [];
parse_byte_range(#{attributes := #file_attr{size = Size}} = State, [First | Rest]) ->
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
        ValidRange -> [ValidRange | parse_byte_range(State, Rest)]
    end.

%%--------------------------------------------------------------------
%% @doc
%% Parses content-type header to mimetype and charset part, if charset
%% is other than utf-8, function returns undefined
%% @end
%%--------------------------------------------------------------------
-spec parse_content(binary()) -> {Mimetype :: binary(), Encoding :: binary() | undefined}.
parse_content(Content) ->
    case binary:split(Content,<<";">>) of
        [RawMimetype, RawEncoding] ->
            case binary:split(utils:trim_spaces(RawEncoding),<<"=">>) of
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

%%--------------------------------------------------------------------
%% @doc Extract the CDMI version from request arguments string.
%%--------------------------------------------------------------------
-spec get_supported_version(list() | binary()) ->
    binary() | undefined.
get_supported_version(undefined) -> undefined;
get_supported_version(VersionBinary) when is_binary(VersionBinary) ->
    VersionList = lists:map(fun utils:trim_spaces/1, binary:split(VersionBinary, <<",">>, [global])),
    get_supported_version(VersionList);
get_supported_version([]) -> throw(?unsupported_version);
get_supported_version([<<"1.1.1">> | _Rest]) -> <<"1.1.1">>;
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
        fun(Opt) ->
            case binary:split(Opt, <<":">>) of
                [SimpleOpt] -> SimpleOpt;
                [SimpleOpt, Range] ->
                    case binary:split(Range, <<"-">>) of
                        [SimpleVal] -> {SimpleOpt, SimpleVal};
                        [From, To] ->
                            {SimpleOpt, binary_to_integer(From), binary_to_integer(To)}
                    end
            end
        end,
        Opts
    ).

%%--------------------------------------------------------------------
%% @doc Validates correctness of request's body.
%%--------------------------------------------------------------------
-spec validate_body(list()) -> ok | no_return().
validate_body(Body) ->
    Keys = proplists:get_keys(Body),
    KeySet = sets:from_list(Keys),
    ExclusiveRequiredKeysSet = sets:from_list(?KEYS_REQUIRED_TO_BE_EXCLUSIVE),
    case length(Keys) =:= length(Body) of
        true ->
            case sets:size(sets:intersection(KeySet, ExclusiveRequiredKeysSet)) of
                N when N > 1 -> throw(?conflicting_body_fields);
                _ -> ok
            end;
        false -> throw(?duplicated_body_fields)
    end.

%%--------------------------------------------------------------------
%% @doc Checks if this objectid request points to capability object
%%--------------------------------------------------------------------
-spec is_capability_object(req()) -> boolean().
is_capability_object(Req) -> %todo return req object
    {Path, _} = cowboy_req:path(Req),
    case binary:split(Path, <<"/">>, [global]) of
        [<<"">>, <<"cdmi">>, <<"cdmi_objectid">>, Id | _Rest] ->
            proplists:is_defined(Id, ?CapabilityPathById);
        _ -> false
    end.
