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

-include("modules/http_worker/rest/cdmi/cdmi_capabilities.hrl").
-include("modules/http_worker/rest/cdmi/cdmi_errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

-define(infinity, 10000000000000).

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
prepare([<<"metadata">> | Tail], #{path := Path, attributes := Attrs} = State) ->
    [{<<"metadata">>, cdmi_metadata:prepare_metadata(Path, Attrs)} | prepare(Tail, State)];
prepare([{<<"metadata">>, Prefix} | Tail], #{path := Path, attributes := Attrs} = State) ->
    [{<<"metadata">>, cdmi_metadata:prepare_metadata(Path, Prefix, Attrs)} | prepare(Tail, State)];
prepare([<<"childrenrange">> | Tail], #{options := Opts, path := Path, auth := Auth} = State) ->
    {ok, ChildNum} = onedata_file_api:get_children_count(Auth, {path, Path}),
    {From, To} =
        case lists:keyfind(<<"children">>, 1, Opts) of
            {<<"children">>, Begin, End} ->
                normalize_childrenrange(Begin, End, ChildNum);
            _ -> case ChildNum of
                     0 -> {undefined, undefined}
%%                      _ -> {0, ChildNum - 1} % todo implement onedata_file_api:get_children_count and uncomment
                 end
        end,
    BinaryRange =
        case {From, To} of
            {undefined, undefined} -> <<"">>;
            _ ->
                <<(integer_to_binary(From))/binary, "-", (integer_to_binary(To))/binary>>
        end,
    [{<<"childrenrange">>, BinaryRange} | prepare(Tail, State)];
prepare([{<<"children">>, From, To} | Tail], #{path := Path, auth := Auth} = State) ->
    {ok, ChildNum} = onedata_file_api:get_children_count(Auth, {path, Path}),
    {From1, To1} = normalize_childrenrange(From, To, ChildNum),
    {ok, List} = onedata_file_api:ls(Auth, {path, Path}, To1 - From1 + 1, From1),
    Children = lists:map(
        fun({_Uuid, Name}) -> %todo distinguish dirs and files
            str_utils:ensure_ends_with_slash(Name)
        end, List),
    [{<<"children">>, Children} | prepare(Tail, State)];
prepare([<<"children">> | Tail], #{path := Path, auth := Auth} = State) ->
    {ok, List} = onedata_file_api:ls(Auth, {path, Path}, ?infinity, 0),
    Children = lists:map(
        fun({_Uuid, Name}) -> %todo distinguish dirs and files
            str_utils:ensure_ends_with_slash(Name)
        end, List),
    [{<<"children">>, Children} | prepare(Tail, State)];
prepare([_Other | Tail], State) ->
    prepare(Tail, State).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Checks if given childrange is correct according to child number. Tries to correct the result
%%--------------------------------------------------------------------
-spec normalize_childrenrange(From :: integer(), To :: integer(), ChildNum :: integer()) ->
    {NewFrom :: integer(), NewTo :: integer()} | no_return().
normalize_childrenrange(From, To, _ChildNum) when From > To ->
    throw(?invalid_childrenrange);
normalize_childrenrange(_From, To, ChildNum) when To >= ChildNum ->
    throw(?invalid_childrenrange);
normalize_childrenrange(From, To, ChildNum) ->
    {From, min(ChildNum - 1, To)}.
