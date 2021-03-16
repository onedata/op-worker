%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for management of file_meta representation
%%% of posix links.
%%% Note: in posix link is equal to hardlink.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_hardlinks).
-author("Michal Wrzeszcz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/posix/errno.hrl").

%% API
-export([new_doc/4, merge_link_and_file_doc/2,
    register_link/2, deregister_link/2,
    get_reference_count/1, get_references/1,
    merge_links_maps/2]).

-type link() :: file_meta:uuid().
-type links_map() :: #{oneprovider:id() => link()}.
-export_type([link/0, links_map/0]).

% TODO VFS-7441 - Test number of links that can be stored in file_meta doc
-define(MAX_LINKS_NUM, 65536). % 64 * 1024

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates document for link.
%% NOTE: when link document is requested it is merged using document representing
%% link and document representing target file (see get_including_deleted/1).
%% @end
%%--------------------------------------------------------------------
-spec new_doc(file_meta:uuid(), file_meta:name(), file_meta:uuid(), od_space:id()) -> file_meta:doc().
new_doc(FileUuid, FileName, ParentUuid, SpaceId) ->
    #document{
        key = fslogic_uuid:gen_link_uuid(FileUuid),
        value = #file_meta{
            name = FileName,
            type = ?LINK_TYPE,
            parent_uuid = ParentUuid,
            provider_id = oneprovider:get_id()
        },
        scope = SpaceId
    }.

-spec merge_link_and_file_doc(file_meta:doc(), file_meta:doc()) -> {ok, file_meta:doc()}.
merge_link_and_file_doc(LinkDoc = #document{value = LinkRecord}, #document{value = FileRecord}) ->
    {ok, LinkDoc#document{
        value = LinkRecord#file_meta{
            mode = FileRecord#file_meta.mode,
            acl = FileRecord#file_meta.acl,
            owner = FileRecord#file_meta.owner,
            links = FileRecord#file_meta.links
        }
    }}.

-spec register_link(file_meta:uuid(), link()) -> {ok, file_meta:doc()} | {error, term()}.
register_link(FileUuid, LinkUuid) ->
    ProviderId = oneprovider:get_id(),
    file_meta:update(FileUuid, fun(#file_meta{links = Links} = Record) ->
        case get_reference_map_size(Links) of
            LinksNum when LinksNum > ?MAX_LINKS_NUM ->
                {error, ?EMLINK};
            _ ->
                ProviderLinks = maps:get(ProviderId, Links, []),
                {ok, Record#file_meta{links = Links#{ProviderId => [LinkUuid | ProviderLinks]}}}
        end
    end).

-spec deregister_link(file_meta:uuid(), link()) -> {ok, non_neg_integer()} | {error, term()}.
deregister_link(FileUuid, LinkUuid) ->
    ProviderId = oneprovider:get_id(),
    Ans = file_meta:update(FileUuid, fun(#file_meta{links = Links} = Record) ->
        ProviderLinks = maps:get(ProviderId, Links, []),
        {ok, Record#file_meta{links = Links#{ProviderId => ProviderLinks -- [LinkUuid]}}}
    end),

    case Ans of
        {ok, Doc}->
            get_reference_count(Doc);
        Other ->
            Other
    end.

-spec get_reference_count(file_meta:uuid() | file_meta:doc()) -> {ok, non_neg_integer()} | {error, term()}.
get_reference_count(#document{deleted = Del1, value = #file_meta{links = Links, deleted = Del2}}) when Del1 or Del2 ->
    {ok, get_reference_map_size(Links)};
get_reference_count(#document{value = #file_meta{links = Links}}) ->
    {ok, get_reference_map_size(Links) + 1};
get_reference_count(Key) ->
    case file_meta:get_including_deleted(Key) of
        {ok, Doc} -> get_reference_count(Doc);
        Other -> Other
    end.

-spec get_references(file_meta:uuid() | file_meta:doc()) -> {ok, [link() | file_meta:uuid()]} | {error, term()}.
get_references(#document{deleted = Del1, value = #file_meta{links = Links, deleted = Del2}}) when Del1 or Del2 ->
    {ok, get_references_from_map(Links)};
get_references(#document{key = MainDocKey, value = #file_meta{links = Links}}) ->
    {ok, [MainDocKey | get_references_from_map(Links)]};
get_references(Key) ->
    case file_meta:get_including_deleted(Key) of
        {ok, Doc} -> get_references(Doc);
        Other -> Other
    end.

-spec merge_links_maps(file_meta:doc(), file_meta:doc()) -> not_mutated | {mutated, links_map()}.
merge_links_maps(#document{mutators = [Mutator | _], value = #file_meta{links = NewLinks}},
    #document{value = #file_meta{links = OldLinks}}) ->
    ChangedMutatorLinks = maps:get(Mutator, NewLinks, []),
    OldMutatorLinks = maps:get(Mutator, OldLinks, []),

    case ChangedMutatorLinks of
        OldMutatorLinks -> not_mutated;
        _ -> {mutated, OldLinks#{Mutator => ChangedMutatorLinks}}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_references_from_map(links_map()) -> [link()].
get_references_from_map(Links) ->
    % Note - do not use lists:flatten as it traverses sublists and it is not necessary here
    lists:flatmap(fun(ProviderLinks) -> ProviderLinks end, maps:values(Links)).

-spec get_reference_map_size(links_map()) -> non_neg_integer().
get_reference_map_size(Links) ->
    lists:foldl(fun(ProviderLinks, Acc) -> length(ProviderLinks) + Acc end, 0, maps:values(Links)).