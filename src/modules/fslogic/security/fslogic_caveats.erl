%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for verification of data caveats.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_caveats).
-author("Bartosz Walkowicz").

-include("proto/common/credentials.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/aai/caveats.hrl").

%% API
-export([
    assert_none_data_caveats/1,
    verify_data_location_caveats/3
]).

-define(DATA_CAVEATS, [cv_data_path, cv_data_objectid]).
-define(DATA_LOCATION_CAVEATS, [cv_data_path, cv_data_objectid]).

-define(CV_PATH(__PATHS), #cv_data_path{whitelist = __PATHS}).
-define(CV_OBJECTID(__OBJECTIDS), #cv_data_objectid{whitelist = __OBJECTIDS}).


-type relation() :: subpath | ancestor.
-type children() :: gb_sets:set(file_meta:name()).
-type data_location_caveat() :: #cv_data_objectid{} | #cv_data_path{}.

-export_type([relation/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec assert_none_data_caveats([caveats:caveat()]) -> ok | no_return().
assert_none_data_caveats(Caveats) when is_list(Caveats) ->
    case caveats:filter(?DATA_CAVEATS, Caveats) of
        [] -> ok;
        _ -> throw(?ERROR_TOKEN_INVALID)
    end;
assert_none_data_caveats(SerializedToken) ->
    case tokens:deserialize(SerializedToken) of
        {ok, Token} ->
            assert_none_data_caveats(tokens:get_caveats(Token));
        {error, _} ->
            throw(?ERROR_TOKEN_INVALID)
    end.


%%--------------------------------------------------------------------
%% @doc
%% TODO WRITEME
%% @end
%%--------------------------------------------------------------------
-spec verify_data_location_caveats(file_ctx:ctx(), [data_location_caveat()],
    AllowAncestors :: boolean()
) ->
    {relation(), file_ctx:ctx(), undefined | children()} | no_return().
verify_data_location_caveats(FileCtx0, Caveats, AllowAncestors) ->
    lists:foldl(fun(Caveat, {CurrRelation, FileCtx1, NamesAcc}) ->
        {CaveatRelation, FileCtx2, Names} = verify_data_location_caveat(
            FileCtx1, Caveat, AllowAncestors
        ),
        case {CaveatRelation, CurrRelation} of
            {subpath, ancestor} ->
                {ancestor, FileCtx2, NamesAcc};
            {subpath, _} ->
                {subpath, FileCtx2, NamesAcc};
            {ancestor, ancestor} ->
                {ancestor, FileCtx2, gb_sets:intersection(Names, NamesAcc)};
            {ancestor, _} ->
                {ancestor, FileCtx2, Names}
        end
    end, {undefined, FileCtx0, undefined}, Caveats).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec verify_data_location_caveat(file_ctx:ctx(), data_location_caveat(),
    AllowAncestors :: boolean()
) ->
    {relation(), file_ctx:ctx(), undefined | children()} | no_return().
verify_data_location_caveat(FileCtx0, ?CV_PATH(AllowedPaths), false) ->
    {FilePath, FileCtx1} = file_ctx:get_canonical_path(FileCtx0),

    IsFileInAllowedSubPath = lists:any(fun(AllowedPath) ->
        str_utils:binary_starts_with(FilePath, AllowedPath)
    end, AllowedPaths),

    case IsFileInAllowedSubPath of
        true ->
            {subpath, FileCtx1, undefined};
        false ->
            throw(?EACCES)
    end;
verify_data_location_caveat(FileCtx, ?CV_PATH(AllowedPaths), false) ->
    check_data_path_relation(FileCtx, AllowedPaths);

verify_data_location_caveat(FileCtx0, ?CV_OBJECTID(ObjectIds), false) ->
    AllowedGuids = objectids_to_guids(ObjectIds),
    {AncestorsGuids, FileCtx1} = file_ctx:get_ancestors_guids(FileCtx0),

    IsFileInAllowedSubPath = lists:any(fun(Guid) ->
        lists:member(Guid, AllowedGuids)
    end, [file_ctx:get_guid_const(FileCtx1) | AncestorsGuids]),

    case IsFileInAllowedSubPath of
        true ->
            {subpath, FileCtx1, undefined};
        false ->
            throw(?EACCES)
    end;
verify_data_location_caveat(FileCtx, ?CV_OBJECTID(ObjectIds), true) ->
    check_data_path_relation(FileCtx, objectids_to_paths(ObjectIds)).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether file is ancestor or subpath to any of specified paths.
%% In case when file is ancestor to one path and subpath to another, then
%% subpath takes precedence.
%% Additionally, if it is ancestor, it returns list of it's immediate
%% children.
%% @end
%%--------------------------------------------------------------------
-spec check_data_path_relation(file_ctx:ctx(), [file_meta:path()]) ->
    {relation(), file_ctx:ctx(), undefined | children()} | no_return().
check_data_path_relation(FileCtx0, AllowedPaths) ->
    {FilePath, FileCtx1} = file_ctx:get_canonical_path(FileCtx0),
    FilePathLen = size(FilePath),

    {Relation, Names} = lists:foldl(fun
        (_AllowedPath, {subpath, _NamesAcc} = Acc) ->
            Acc;
        (AllowedPath, {_Relation, NamesAcc} = Acc) ->
            AllowedPathLen = size(AllowedPath),
            case FilePathLen >= AllowedPathLen of
                true ->
                    % Check if FilePath is allowed by caveats
                    % (AllowedPath is ancestor to FilePath)
                    case str_utils:binary_starts_with(FilePath, AllowedPath) of
                        true -> {subpath, undefined};
                        false -> Acc
                    end;
                false ->
                    % Check if FilePath is ancestor to AllowedPath
                    case AllowedPath of
                        <<FilePath:FilePathLen/binary, "/", SubPath/binary>> ->
                            [Name | _] = string:split(SubPath, <<"/">>),
                            {ancestor, gb_sets:add(Name, utils:ensure_defined(
                                NamesAcc, undefined, gb_sets:new()
                            ))};
                        _ ->
                            Acc
                    end
            end
    end, {undefined, undefined}, AllowedPaths),

    case Relation of
        undefined ->
            throw(?EACCES);
        _ ->
            {Relation, FileCtx1, Names}
    end.


%% @private
-spec objectids_to_guids([file_id:objectid()]) -> [file_id:file_guid()].
objectids_to_guids(Objectids) ->
    lists:foldl(fun(ObjectId, Guids) ->
        try
            {ok, Guid} = file_id:objectid_to_guid(ObjectId),
            [Guid | Guids]
        catch _:_ ->
            % Invalid objectid does not make entire token invalid
            Guids
        end
    end, [], Objectids).


%% @private
-spec objectids_to_paths([file_id:objectid()]) -> [file_meta:path()].
objectids_to_paths(ObjectIds) ->
    lists:foldl(fun(ObjectId, Paths) ->
        try
            {ok, Guid} = file_id:objectid_to_guid(ObjectId),
            FileCtx = file_ctx:new_by_guid(Guid),
            {Path, _} = file_ctx:get_canonical_path(FileCtx),
            [Path | Paths]
        catch _:_ ->
            % File may have been deleted so it is not possible to resolve
            % it's path so skip it (it does not make token invalid)
            Paths
        end
    end, [], ObjectIds).
