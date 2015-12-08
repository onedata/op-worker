%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Prepare answer for cdmi container request.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_container_answer).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/http_worker/rest/cdmi/cdmi_capabilities.hrl").
-include("modules/http_worker/rest/cdmi/cdmi_errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% API
-export([prepare/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Prepares proplist formatted answer with field names from given list of binaries
%%--------------------------------------------------------------------
-spec prepare([FieldName :: binary()], #{}) -> [{FieldName :: binary(), Value :: term()}].
prepare([], _State) ->
    [];
prepare([<<"objectType">> | Tail], State) ->
    [{<<"objectType">>, <<"application/cdmi-container">>} | prepare(Tail, State)];
%% prepare([<<"objectID">> | Tail], #{attributes := #file_attr{uuid = Uuid}} = State) -> todo introduce objectid
%%     [{<<"objectID">>, cdmi_id:uuid_to_objectid(Uuid)} | prepare(Tail, State)];
prepare([<<"objectName">> | Tail], #{path := Path} = State) ->
    [{<<"objectName">>, <<(filename:basename(Path))/binary, "/">>} | prepare(Tail, State)];
prepare([<<"parentURI">> | Tail], #{path := <<"/">>} = State) ->
    [{<<"parentURI">>, <<>>} | prepare(Tail, State)];
prepare([<<"parentURI">> | Tail], #{path := Path} = State) ->
    ParentURI = str_utils:ensure_ends_with_slash(
        filename:dirname(binary:part(Path, {0, byte_size(Path) - 1}))),
    [{<<"parentURI">>, ParentURI} | prepare(Tail, State)];
%% prepare([<<"parentID">> | Tail], #{path := <<"/">>} = State) -> todo introduce objectid
%%     prepare(Tail, State);
%% prepare([<<"parentID">> | Tail], #{path := Path, auth := Auth} = State) ->
%%     {ok, #file_attr{uuid = Uuid}} =
%%         onedata_file_api:stat(Auth, {path, filename:dirname(binary:part(Path, {0, byte_size(Path) - 1}))}),
%%     [{<<"parentID">>, cdmi_id:uuid_to_objectid(Uuid)} | prepare(Tail, State)];
prepare([<<"capabilitiesURI">> | Tail], State) ->
    [{<<"capabilitiesURI">>, ?container_capability_path} | prepare(Tail, State)];
prepare([<<"completionStatus">> | Tail], State) ->
    [{<<"completionStatus">>, <<"Complete">>} | prepare(Tail, State)];
prepare([<<"metadata">> | Tail], #{auth := Auth, attributes := Attrs = #file_attr{uuid = Uuid}} = State) ->
    [{<<"metadata">>, cdmi_metadata:prepare_metadata(Auth, {uuid, Uuid}, Attrs)} | prepare(Tail, State)];
prepare([{<<"metadata">>, Prefix} | Tail], #{auth := Auth, attributes := Attrs = #file_attr{uuid = Uuid}} = State) ->
    [{<<"metadata">>, cdmi_metadata:prepare_metadata(Auth, {uuid, Uuid}, Prefix, Attrs)} | prepare(Tail, State)];
prepare([<<"childrenrange">> | Tail], #{options := Opts, attributes := #file_attr{uuid = Uuid}, auth := Auth} = State) ->
    {ok, ChildNum} = onedata_file_api:get_children_count(Auth, {uuid, Uuid}),
    {From, To} =
        case lists:keyfind(<<"children">>, 1, Opts) of
            {<<"children">>, Begin, End} ->
                {ok, MaxChildren} = application:get_env(?APP_NAME, max_children_per_request),
                normalize_childrenrange(Begin, End, ChildNum, MaxChildren);
            _ -> case ChildNum of
                     0 -> {undefined, undefined};
                     _ -> {0, ChildNum - 1}
                 end
        end,
    BinaryRange =
        case {From, To} of
            {undefined, undefined} -> <<"">>;
            _ ->
                <<(integer_to_binary(From))/binary, "-", (integer_to_binary(To))/binary>>
        end,
    [{<<"childrenrange">>, BinaryRange} | prepare(Tail, State)];
prepare([{<<"children">>, From, To} | Tail], #{attributes := #file_attr{uuid = Uuid}, auth := Auth} = State) ->
    {ok, MaxChildren} = application:get_env(?APP_NAME, max_children_per_request),
    {ok, ChildNum} = onedata_file_api:get_children_count(Auth, {uuid, Uuid}),
    {From1, To1} = normalize_childrenrange(From, To, ChildNum, MaxChildren),
    {ok, List} = onedata_file_api:ls(Auth, {uuid, Uuid}, To1 - From1 + 1, From1),
    Children = lists:map(
        fun({Uuid, Name}) -> distinguish_files(Uuid, Name, Auth) end, List),
    [{<<"children">>, Children} | prepare(Tail, State)];
prepare([<<"children">> | Tail], #{attributes := #file_attr{uuid = Uuid}, auth := Auth} = State) ->
    {ok, MaxChildren} = application:get_env(?APP_NAME, max_children_per_request),
    {ok, List} = onedata_file_api:ls(Auth, {uuid, Uuid}, MaxChildren + 1, 0),
    terminate_if_too_many_children(List, MaxChildren),
    Children = lists:map(
        fun({Uuid, Name}) -> distinguish_files(Uuid, Name, Auth) end, List),
    [{<<"children">>, Children} | prepare(Tail, State)];
prepare([_Other | Tail], State) ->
    prepare(Tail, State).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Distinguishes regular files from directories
%% (for regular files returns path ending with slash)
%% @end
%%--------------------------------------------------------------------
-spec distinguish_files(Uuid :: onedata_file_api:file_uuid(), Name :: binary() ,
    Auth::onedata_auth_api:auth()) -> binary().
distinguish_files(Uuid, Name, Auth) ->
    case onedata_file_api:stat(Auth, {uuid, Uuid}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} ->
            str_utils:ensure_ends_with_slash(Name);
        {ok, _} -> Name
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if given childrange is correct according to child number.
%% Tries to correct the result
%% @end
%%--------------------------------------------------------------------
-spec normalize_childrenrange(From :: integer(), To :: integer(),
  ChildNum :: integer(), MaxChildren :: integer()) ->
    {NewFrom :: integer(), NewTo :: integer()} | no_return().
normalize_childrenrange(From, To, _ChildNum, _MaxChildren) when From > To ->
    throw(?invalid_childrenrange);
normalize_childrenrange(_From, To, ChildNum, _MaxChildren) when To >= ChildNum ->
    throw(?invalid_childrenrange);
normalize_childrenrange(From, To, ChildNum, MaxChildren) ->
    To2 = min(ChildNum - 1, To),
    case MaxChildren < (To2 - From + 1) of
        true -> throw(?too_large_childrenrange(MaxChildren));
        false -> {From, To2}
    end.

%%--------------------------------------------------------------------
%% @doc Terminates request with error if requested childrenrange exceeds system limit
%%--------------------------------------------------------------------
-spec terminate_if_too_many_children(list(), non_neg_integer()) -> ok | no_return().
terminate_if_too_many_children(List, MaxChildren) when length(List) > MaxChildren ->
    throw(?too_large_childrenrange(MaxChildren));
terminate_if_too_many_children(_, _) ->
    ok.
