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
-include("http/rest/cdmi/cdmi_capabilities.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% API
-export([prepare/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Prepares proplist formatted answer with field names from given list of binaries
%%--------------------------------------------------------------------
-spec prepare([FieldName :: binary()], maps:map()) -> maps:map().
prepare([], _State) ->
    #{};
prepare([<<"objectType">> | Tail], State) ->
    (prepare(Tail, State))#{<<"objectType">> => <<"application/cdmi-container">>};
prepare([<<"objectID">> | Tail], #{guid := Uuid} = State) ->
    {ok, Id} = cdmi_id:uuid_to_objectid(Uuid),
    (prepare(Tail, State))#{<<"objectID">> => Id};
prepare([<<"objectName">> | Tail], #{path := Path} = State) ->
    (prepare(Tail, State))#{<<"objectName">> => <<(filename:basename(Path))/binary, "/">>};
prepare([<<"parentURI">> | Tail], #{path := <<"/">>} = State) ->
    (prepare(Tail, State))#{<<"parentURI">> => <<>>};
prepare([<<"parentURI">> | Tail], #{path := Path} = State) ->
    (prepare(Tail, State))#{<<"parentURI">> => filepath_utils:parent_dir(Path)};
prepare([<<"parentID">> | Tail], #{path := <<"/">>} = State) ->
    prepare(Tail, State);
prepare([<<"parentID">> | Tail], #{path := Path, auth := Auth} = State) ->
    {ok, #file_attr{uuid = Uuid}} = onedata_file_api:stat(Auth, {path, filepath_utils:parent_dir(Path)}),
    {ok, Id} = cdmi_id:uuid_to_objectid(Uuid),
    (prepare(Tail, State))#{<<"parentID">> => Id};
prepare([<<"capabilitiesURI">> | Tail], State) ->
    (prepare(Tail, State))#{<<"capabilitiesURI">> => ?container_capability_path};
prepare([<<"completionStatus">> | Tail], State) ->
    (prepare(Tail, State))#{<<"completionStatus">> => <<"Complete">>};
prepare([<<"metadata">> | Tail], #{auth := Auth, attributes := Attrs = #file_attr{uuid = Uuid}} = State) ->
    (prepare(Tail, State))#{<<"metadata">> => cdmi_metadata:prepare_metadata(Auth, {guid, Uuid}, <<>>, Attrs)};
prepare([{<<"metadata">>, Prefix} | Tail], #{auth := Auth, attributes := Attrs = #file_attr{uuid = Uuid}} = State) ->
    (prepare(Tail, State))#{<<"metadata">> => cdmi_metadata:prepare_metadata(Auth, {guid, Uuid}, Prefix, Attrs)};
prepare([<<"metadata">> | Tail], #{auth := Auth, guid := Uuid} = State) ->
    (prepare(Tail, State))#{<<"metadata">> => cdmi_metadata:prepare_metadata(Auth, {guid, Uuid})};
prepare([{<<"metadata">>, Prefix} | Tail], #{auth := Auth, guid := Uuid} = State) ->
    (prepare(Tail, State))#{<<"metadata">> => cdmi_metadata:prepare_metadata(Auth, {guid, Uuid}, Prefix)};
prepare([<<"childrenrange">> | Tail], #{options := Opts, guid := Uuid, auth := Auth} = State) ->
    {ok, ChildNum} = onedata_file_api:get_children_count(Auth, {guid, Uuid}),
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
    (prepare(Tail, State))#{<<"childrenrange">> => BinaryRange};
prepare([{<<"children">>, From, To} | Tail], #{guid := Uuid, auth := Auth} = State) ->
    {ok, MaxChildren} = application:get_env(?APP_NAME, max_children_per_request),
    {ok, ChildNum} = onedata_file_api:get_children_count(Auth, {guid, Uuid}),
    {From1, To1} = normalize_childrenrange(From, To, ChildNum, MaxChildren),
    {ok, List} = onedata_file_api:ls(Auth, {guid, Uuid}, From1, To1 - From1 + 1),
    Children = lists:map(
        fun({Uuid, Name}) -> distinguish_files(Uuid, Name, Auth) end, List),
    (prepare(Tail, State))#{<<"children">> => Children};
prepare([<<"children">> | Tail], #{guid := Uuid, auth := Auth} = State) ->
    {ok, MaxChildren} = application:get_env(?APP_NAME, max_children_per_request),
    {ok, List} = onedata_file_api:ls(Auth, {guid, Uuid}, 0, MaxChildren + 1),
    terminate_if_too_many_children(List, MaxChildren),
    Children = lists:map(
        fun({Uuid, Name}) -> distinguish_files(Uuid, Name, Auth) end, List),
    (prepare(Tail, State))#{<<"children">> => Children};
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
-spec distinguish_files(Uuid :: onedata_file_api:file_guid(), Name :: binary(),
    Auth :: onedata_auth_api:auth()) -> binary().
distinguish_files(Uuid, Name, Auth) ->
    case onedata_file_api:stat(Auth, {guid, Uuid}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} ->
            filepath_utils:ensure_ends_with_slash(Name);
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
    throw(?ERROR_INVALID_CHILDRENRANGE);
normalize_childrenrange(_From, To, ChildNum, _MaxChildren) when To >= ChildNum ->
    throw(?ERROR_INVALID_CHILDRENRANGE);
normalize_childrenrange(From, To, ChildNum, MaxChildren) ->
    To2 = min(ChildNum - 1, To),
    case MaxChildren < (To2 - From + 1) of
        true -> throw(?ERROR_TOO_LARGE_CHILDRENRANGE(MaxChildren));
        false -> {From, To2}
    end.

%%--------------------------------------------------------------------
%% @doc Terminates request with error if requested childrenrange exceeds system limit
%%--------------------------------------------------------------------
-spec terminate_if_too_many_children(list(), non_neg_integer()) -> ok | no_return().
terminate_if_too_many_children(List, MaxChildren) when length(List) > MaxChildren ->
    throw(?ERROR_TOO_LARGE_CHILDRENRANGE(MaxChildren));
terminate_if_too_many_children(_, _) ->
    ok.