%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for management of file_meta hardlinks.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_hardlinks).
-author("Michal Wrzeszcz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/posix/errno.hrl").

%% API
-export([new_hardlink_doc/4, merge_hardlink_and_file_doc/2,
    register_hardlink/2, deregister_hardlink/2,
    get_reference_count/1, get_references/1,
    merge_hardlinks_maps/2]).

-type hardlink() :: file_meta:uuid().
-type hardlinks_map() :: #{oneprovider:id() => hardlink()}.
-export_type([hardlink/0, hardlinks_map/0]).

% TODO VFS-7441 - Test number of links that can be stored in file_meta doc
-define(MAX_LINKS_NUM, 65536). % 64 * 1024

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates document for hardlink.
%% NOTE: when hardlink document is requested it is merged using document
%% representing hardlink and document representing target file (see get_including_deleted/1).
%% @end
%%--------------------------------------------------------------------
-spec new_hardlink_doc(file_meta:uuid(), file_meta:name(), file_meta:uuid(), od_space:id()) -> file_meta:doc().
new_hardlink_doc(FileUuid, FileName, ParentUuid, SpaceId) ->
    #document{
        key = fslogic_uuid:gen_hardlink_uuid(FileUuid),
        value = #file_meta{
            name = FileName,
            type = ?HARDLINK_TYPE,
            parent_uuid = ParentUuid,
            provider_id = oneprovider:get_id()
        },
        scope = SpaceId
    }.

-spec merge_hardlink_and_file_doc(file_meta:doc(), file_meta:doc()) -> {ok, file_meta:doc()}.
merge_hardlink_and_file_doc(LinkDoc = #document{value = LinkRecord}, #document{value = FileRecord}) ->
    {ok, LinkDoc#document{
        value = LinkRecord#file_meta{
            mode = FileRecord#file_meta.mode,
            acl = FileRecord#file_meta.acl,
            owner = FileRecord#file_meta.owner,
            hardlinks = FileRecord#file_meta.hardlinks
        }
    }}.

-spec register_hardlink(file_meta:uuid(), hardlink()) -> {ok, file_meta:doc()} | {error, term()}.
register_hardlink(FileUuid, LinkUuid) ->
    ProviderId = oneprovider:get_id(),
    file_meta:update(FileUuid, fun(#file_meta{hardlinks = Links} = Record) ->
        case get_reference_map_size(Links) of
            LinksNum when LinksNum > ?MAX_LINKS_NUM ->
                {error, ?EMLINK};
            _ ->
                ProviderLinks = maps:get(ProviderId, Links, []),
                {ok, Record#file_meta{hardlinks = Links#{ProviderId => [LinkUuid | ProviderLinks]}}}
        end
    end).

-spec deregister_hardlink(file_meta:uuid(), hardlink()) -> {ok, non_neg_integer()} | {error, term()}.
deregister_hardlink(FileUuid, LinkUuid) ->
    ProviderId = oneprovider:get_id(),
    Ans = file_meta:update(FileUuid, fun(#file_meta{hardlinks = Links} = Record) ->
        ProviderLinks = maps:get(ProviderId, Links, []),
        {ok, Record#file_meta{hardlinks = Links#{ProviderId => ProviderLinks -- [LinkUuid]}}}
    end),

    case Ans of
        {ok, Doc}->
            get_reference_count(Doc);
        Other ->
            Other
    end.

-spec get_reference_count(file_meta:uuid() | file_meta:doc()) -> {ok, non_neg_integer()} | {error, term()}.
get_reference_count(#document{deleted = Del1, value = #file_meta{hardlinks = Links, deleted = Del2}}) when Del1 or Del2 ->
    {ok, get_reference_map_size(Links)};
get_reference_count(#document{value = #file_meta{hardlinks = Links}}) ->
    {ok, get_reference_map_size(Links) + 1};
get_reference_count(Key) ->
    case file_meta:get_including_deleted(Key) of
        {ok, Doc} -> get_reference_count(Doc);
        Other -> Other
    end.

-spec get_references(file_meta:uuid() | file_meta:doc()) -> {ok, [hardlink() | file_meta:uuid()]} | {error, term()}.
get_references(#document{deleted = Del1, value = #file_meta{hardlinks = Links, deleted = Del2}}) when Del1 or Del2 ->
    {ok, get_references_from_map(Links)};
get_references(#document{key = MainDocKey, value = #file_meta{hardlinks = Links}}) ->
    {ok, [MainDocKey | get_references_from_map(Links)]};
get_references(Key) ->
    case file_meta:get_including_deleted(Key) of
        {ok, Doc} -> get_references(Doc);
        Other -> Other
    end.

-spec merge_hardlinks_maps(file_meta:doc(), file_meta:doc()) -> not_mutated | {mutated, hardlinks_map()}.
merge_hardlinks_maps(#document{mutators = [Mutator | _], value = #file_meta{hardlinks = NewHardlinks}},
    #document{value = #file_meta{hardlinks = OldHardlinks}}) ->
    ChangedMutatorHardlinks = maps:get(Mutator, NewHardlinks, []),
    OldMutatorHardlinks = maps:get(Mutator, OldHardlinks, []),

    case ChangedMutatorHardlinks of
        OldMutatorHardlinks -> not_mutated;
        _ -> {mutated, OldHardlinks#{Mutator => ChangedMutatorHardlinks}}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_references_from_map(hardlinks_map()) -> [hardlink()].
get_references_from_map(Links) ->
    % Note - do not use lists:flatten as it traverses sublists and it is not necessary here
    lists:flatmap(fun(ProviderLinks) -> ProviderLinks end, maps:values(Links)).

-spec get_reference_map_size(hardlinks_map()) -> non_neg_integer().
get_reference_map_size(Links) ->
    lists:foldl(fun(ProviderLinks, Acc) -> length(ProviderLinks) + Acc end, 0, maps:values(Links)).