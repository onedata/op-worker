%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module providing translation functions for file attr.
%%% @end
%%%--------------------------------------------------------------------
-module(file_attr_translator).
-author("Michal Stanisz").

-include("global_definitions.hrl").

-export([
    to_json/1, to_json/2,
    build_optional_attrs_opt/1,
    select_attrs/2
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec to_json(lfm_attrs:file_attributes()) -> json_utils:json_map().
to_json(#file_attr{
    guid = Guid,
    name = Name,
    mode = Mode,
    parent_guid = ParentGuid,
    uid = Uid,
    gid = Gid,
    atime = Atime,
    mtime = Mtime,
    ctime = Ctime,
    type = Type,
    size = Size,
    shares = Shares,
    provider_id = ProviderId,
    owner_id = OwnerId,
    nlink = HardlinksCount,
    index = Index,
    xattrs = Xattrs
}) ->
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),
    
    BaseJson = #{
        <<"file_id">> => ObjectId,
        <<"name">> => Name,
        <<"mode">> => list_to_binary(string:right(integer_to_list(Mode, 8), 3, $0)),
        <<"parent_id">> => case ParentGuid of
            undefined ->
                null;
            _ ->
                {ok, ParentObjectId} = file_id:guid_to_objectid(ParentGuid),
                ParentObjectId
        end,
        <<"storage_user_id">> => Uid,
        <<"storage_group_id">> => Gid,
        <<"atime">> => Atime,
        <<"mtime">> => Mtime,
        <<"ctime">> => Ctime,
        <<"type">> => str_utils:to_binary(Type),
        <<"size">> => case Type of
            ?DIRECTORY_TYPE -> null;
            _ -> utils:undefined_to_null(Size)
        end,
        <<"shares">> => Shares,
        <<"provider_id">> => ProviderId,
        <<"owner_id">> => OwnerId,
        <<"hardlinks_count">> => utils:undefined_to_null(HardlinksCount),
        <<"index">> => file_listing:encode_index(Index)
    },
    maps:fold(fun(XattrName, XattrValue, Acc) ->
        Acc#{<<"xattr.", XattrName/binary>> => utils:undefined_to_null(XattrValue)}
    end, BaseJson, Xattrs).


-spec to_json(lfm_attrs:file_attributes(), [binary()]) -> json_utils:json_map().
to_json(FileAttrs, RequestedAttributes) ->
    select_attrs(to_json(FileAttrs), RequestedAttributes).


-spec build_optional_attrs_opt([binary()]) -> [attr_req:optional_attr()].
build_optional_attrs_opt(RequiredAttrs) ->
    {OptionalAttrsList, Xattrs} = lists:foldl(fun
        (<<"size">>, {OptionalAttrsListAcc, XattrsAcc}) ->
            {[size | OptionalAttrsListAcc], XattrsAcc};
        (<<"hardlinks_count">>, {OptionalAttrsListAcc, XattrsAcc}) ->
            {[link_count  | OptionalAttrsListAcc], XattrsAcc};
        (<<"xattr.", XattrName/binary>>, {OptionalAttrsListAcc, XattrsAcc}) ->
            {OptionalAttrsListAcc, [XattrName | XattrsAcc]};
        (_, Acc) ->
            Acc
    end, {[], []}, RequiredAttrs),
    case Xattrs of
        [] -> OptionalAttrsList;
        _ -> [{xattrs, Xattrs} | OptionalAttrsList]
    end.


-spec select_attrs(json_utils:json_term(), [binary()]) -> json_utils:json_term().
select_attrs(FileAttrsJson, RequestedAttributes) ->
    maps:with(RequestedAttributes, FileAttrsJson).
