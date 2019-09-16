%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used by qos module.
%%%
%%% QoS management is based on two types of documents synchronized within space:
%%% qos_entry and file_qos.
%%%
%%% The qos_entry document contains information about single QoS requirement
%%% including QoS expression, number of required replicas, UUID of file or
%%% directory for which requirement has been added, information of QoS
%%% requirement can be satisfied, information about traverse requests and
%%% active traverses.
%%% Such document is created when user adds QoS requirement for file or directory.
%%% Requirement added for directory is inherited by whole directory structure.
%%% Each QoS requirement is evaluated separately. It means that it is not
%%% possible to define inconsistent requirements. For example if one requirement
%%% says that file should be present on storage in Poland and other requirement
%%% says that file should be present on storage in any country but Poland,
%%% two different replicas will be created. On the other hand the same file
%%% replica can fulfill multiple different QoS requirements. For example if
%%% there is storage of type disk in Poland, then replica on such storage can
%%% fulfill requirements that demands replica on storage in Poland and requirements
%%% that demands replica on storage of type disk.
%%% Multiple qos_entry documents can be created for the same file or directory.
%%% Adding two identical QoS requirements for the same file results in two
%%% different qos_entry documents.
%%% QoS requirement is considered as fulfilled when:
%%%     - there is no information that QoS requirement cannot be
%%%       satisfied (is_possible field in qos_entry)
%%%     - there are no pending traverse requests
%%%     - all traverse tasks, triggered by creating this QoS requirement, are finished
%%%
%%%
%%% The file_qos item contains aggregated information about QoS defined
%%% for file or directory. It contains:
%%%     - qos_entries - holds IDs of all qos_entries defined for this file (
%%%       including qos_entries which demands cannot be satisfied),
%%%     - target_storages - holds mapping storage_id to list of qos_entry IDs.
%%%       When new QoS is added for file or directory, storages on which replicas
%%%       should be stored are calculated using QoS expression. Then traverse
%%%       requests are added to qos_entry document. When provider notice change
%%%       in qos_entry, it checks whether there is traverse request defined
%%%       its storage. If yes, provider updates target_storages and
%%%       starts traverse.
%%% Each file or directory can be associated with at most one such document.
%%% Getting full information about QoS defined for file or directory requires
%%% calculating effective file_qos as it is inherited from all parents.
%%%
%%%
%%% NOTE!!!
%%% If you introduce any changes in this module, please ensure that
%%% docs in file_qos.erl and qos_entry.erl are up to date.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(QOS_HRL).
-define(QOS_HRL, 1).


-define(QOS_SYNCHRONIZATION_PRIORITY, 224).


% macros used for operations on QoS expression
-define(UNION, <<"|">>).
-define(INTERSECTION, <<"&">>).
% TODO: VFS-5712 for now used "-" instead of "\" as backslash is an escape character,
% have to discuss that
-define(COMPLEMENT, <<"-">>).
-define(OPERATORS, [?UNION, ?INTERSECTION, ?COMPLEMENT]).

-define(L_PAREN, <<"(">>).
-define(R_PAREN, <<")">>).
-define(EQUALITY, <<"=">>).


% macros used for operations on QoS bounded cache
-define(CACHE_TABLE_NAME(SpaceId), binary_to_atom(SpaceId, utf8)).
-define(QOS_BOUNDED_CACHE_GROUP, <<"qos_bonded_cache_group">>).


-define(IMPOSSIBLE_QOS_KEY, <<"impossible_qos_key">>).


% Request to remote providers to start QoS traverse.
-record(qos_traverse_req, {
    % uuid of file that travers should start from
    start_file_uuid :: file_meta:uuid(),
    target_storage :: storage:id()
}).

-endif.
