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
%%% including QoS expression, number of required replicas, fulfillment status
%%% and UUID of file or directory for which requirement has been added.
%%% Such document is created when user adds QoS requirement for file or directory.
%%% Each QoS requirement is evaluated separately. It means that it is not
%%% possible to define inconsistent requirements. For example if one requirement
%%% says that file should be present on storage in Poland and other requirement
%%% says that file should be present on storage in any country but Poland,
%%% two different replicas will be created. On the other hand the same file
%%% replica can fulfill multiple different QoS requirements. For example if
%%% there is storage of type disk in Poland, then replica on such storage can
%%% fulfill requirements that demands replica on storage in Poland and requirements
%%% that demands replica on storage of type disk. System will create new file
%%% replica only if currently existing replicas don't fulfill QoS requirements.
%%% Multiple qos_entry documents can be created for the same file or directory.
%%% Adding two identical QoS requirements for the same file results in two
%%% different qos_entry documents. qos_entry document can be related with links
%%% tree containing ID of transfers scheduled to fulfill QoS after file or
%%% file in directory has been changed.
%%% QoS requirement is considered as fulfilled when:
%%% - there is no active traverse tasks for this QoS
%%% - status indicates that first traverse task has ended
%%%
%%% The file_qos item contains aggregated information about QoS defined for file
%%% or directory. It contains list of qos_entry IDs and mapping that maps storage_id
%%% to list of qos_entry IDs that requires file replica on this storage. Each file
%%% or directory can be related with at most one such document. It is created/updated
%%% in one of following cases:
%%% - new qos_entry document is created for file or directory. In this case
%%%   target storages are chosen according to QoS expression and number of
%%%   required replicas defined in qos_entry document. Then file_qos document is
%%%   updated - qos_entry ID is added to QoS list and target storages mapping,
%%% - target storages for file or directory differs from the one chosen for
%%%   parent directory. In this case file_qos document is updated using
%%%   qos_entry ID and all chosen target storages (not only the ones that
%%%   differs from parent)
%%% According to this getting full information about QoS defined for file or
%%% directory requires calculating effective file_qos as file_qos document
%%% is not created for each file separately.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(QOS_HRL).
-define(QOS_HRL, 1).

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


-define(QOS_BOUNDED_CACHE_GROUP, qos_cache_group).

-define(ERROR_CANNOT_FULFILL_QOS, cannot_fulfill_qos).

-define(IMPOSSIBLE_QOS_KEY, <<"impossible_qos_key">>).

% qos status
-define(QOS_IN_PROGRESS_STATUS, in_progress).
-define(QOS_TRAVERSE_FINISHED_STATUS, traverse_finished).
-define(QOS_IMPOSSIBLE_STATUS, impossible).

-endif.
