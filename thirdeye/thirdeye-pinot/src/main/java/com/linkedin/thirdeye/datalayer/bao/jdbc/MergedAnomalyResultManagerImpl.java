package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.EmailConfigurationBean;
import com.linkedin.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

public class MergedAnomalyResultManagerImpl extends AbstractManagerImpl<MergedAnomalyResultDTO>
    implements MergedAnomalyResultManager {
  // find a conflicting window
  private static final String FIND_BY_COLLECTION_METRIC_DIMENSIONS_TIME =
      " where collection=:collection and metric=:metric " + "and dimensions in (:dimensions) "
          + "and (startTime < :endTime and endTime > :startTime) " + "order by endTime desc";

  // find a conflicting window
  private static final String FIND_BY_COLLECTION_METRIC_TIME =
      "where collection=:collection and metric=:metric "
          + "and (startTime < :endTime and endTime > :startTime) order by endTime desc";

  // find a conflicting window
  private static final String FIND_BY_COLLECTION_TIME = "where collection=:collection "
      + "and (startTime < :endTime and endTime > :startTime) order by endTime desc";

  private static final String FIND_BY_FUNCTION_ID = "where functionId=:functionId";

  private static final String FIND_BY_FUNCTION_AND_DIMENSIONS =
      "where functionId=:functionId " + "and dimensions=:dimensions order by endTime desc limit 1";

  private static final String FIND_BY_FUNCTION_AND_NULL_DIMENSION =
      "where functionId=:functionId " + "and dimensions is null order by endTime desc";

  public MergedAnomalyResultManagerImpl() {
    super(MergedAnomalyResultDTO.class, MergedAnomalyResultBean.class);
  }

  public Long save(MergedAnomalyResultDTO mergedAnomalyResultDTO) {
    if (mergedAnomalyResultDTO.getId() != null) {
      //TODO: throw exception and force the caller to call update instead
      update(mergedAnomalyResultDTO);
      return mergedAnomalyResultDTO.getId();
    }
    MergedAnomalyResultBean mergeAnomalyBean = convertMergeAnomalyDTO2Bean(mergedAnomalyResultDTO);
    Long id = genericPojoDao.put(mergeAnomalyBean);
    mergedAnomalyResultDTO.setId(id);
    return id;
  }

  public int update(MergedAnomalyResultDTO mergedAnomalyResultDTO) {
    if (mergedAnomalyResultDTO.getId() == null) {
      Long id = save(mergedAnomalyResultDTO);
      if (id > 0) {
        return 1;
      } else {
        return 0;
      }
    } else {
      MergedAnomalyResultBean mergeAnomalyBean =
          convertMergeAnomalyDTO2Bean(mergedAnomalyResultDTO);
      return genericPojoDao.update(mergeAnomalyBean);
    }
  }

  public MergedAnomalyResultDTO findById(Long id) {
    MergedAnomalyResultBean mergedAnomalyResultBean =
        genericPojoDao.get(id, MergedAnomalyResultBean.class);
    if (mergedAnomalyResultBean != null) {
      MergedAnomalyResultDTO mergedAnomalyResultDTO;
      mergedAnomalyResultDTO = convertMergedAnomalyBean2DTO(mergedAnomalyResultBean);
      return mergedAnomalyResultDTO;
    } else {
      return null;
    }
  }

  @Override
  public List<MergedAnomalyResultDTO> getAllByTimeEmailIdAndNotifiedFalse(long startTime,
      long endTime, long emailConfigId) {
    EmailConfigurationBean emailConfigurationBean =
        genericPojoDao.get(emailConfigId, EmailConfigurationBean.class);
    List<Long> functionIds = emailConfigurationBean.getFunctionIds();
    if (functionIds == null || functionIds.isEmpty()) {
      return Collections.emptyList();
    }
    Long[] functionIdArray = functionIds.toArray(new Long[] {});
    Predicate predicate = Predicate.AND(//
        Predicate.LT("startTime", endTime), //
        Predicate.GT("endTime", startTime), //
        Predicate.IN("functionId", functionIdArray), //
        Predicate.EQ("notified", false)//
    );
    List<MergedAnomalyResultBean> list =
        genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    List<MergedAnomalyResultDTO> result = new ArrayList<>();
    for (MergedAnomalyResultBean bean : list) {
      result.add(convertMergedAnomalyBean2DTO(bean));
    }
    return result;
  }

  @Override
  public List<MergedAnomalyResultDTO> findByFunctionId(Long functionId) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("functionId", functionId);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(FIND_BY_FUNCTION_ID,
        filterParams, MergedAnomalyResultBean.class);
    List<MergedAnomalyResultDTO> result = new ArrayList<>();
    for (MergedAnomalyResultBean bean : list) {
      result.add(convertMergedAnomalyBean2DTO(bean));
    }
    return result;
  }

  public List<MergedAnomalyResultDTO> findByCollectionMetricDimensionsTime(String collection,
      String metric, String[] dimensions, long startTime, long endTime) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    filterParams.put("metric", metric);
    filterParams.put("dimensions", dimensions[0]);
    filterParams.put("startTime", startTime);
    filterParams.put("endTime", endTime);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_COLLECTION_METRIC_DIMENSIONS_TIME, filterParams, MergedAnomalyResultBean.class);
    List<MergedAnomalyResultDTO> result = new ArrayList<>();
    for (MergedAnomalyResultBean bean : list) {
      result.add(convertMergedAnomalyBean2DTO(bean));
    }
    return result;
  }


  @Override
  public List<MergedAnomalyResultDTO> findByCollectionMetricTime(String collection, String metric,
      long startTime, long endTime) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    filterParams.put("metric", metric);
    filterParams.put("startTime", startTime);
    filterParams.put("endTime", endTime);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_COLLECTION_METRIC_TIME, filterParams, MergedAnomalyResultBean.class);
    List<MergedAnomalyResultDTO> result = new ArrayList<>();
    for (MergedAnomalyResultBean bean : list) {
      result.add(convertMergedAnomalyBean2DTO(bean));
    }
    return result;
  }

  @Override
  public List<MergedAnomalyResultDTO> findByCollectionTime(String collection, long startTime,
      long endTime) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    filterParams.put("startTime", startTime);
    filterParams.put("endTime", endTime);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_COLLECTION_TIME, filterParams, MergedAnomalyResultBean.class);
    List<MergedAnomalyResultDTO> result = new ArrayList<>();
    for (MergedAnomalyResultBean bean : list) {
      result.add(convertMergedAnomalyBean2DTO(bean));
    }
    return result;
  }


  @Override
  public MergedAnomalyResultDTO findLatestByFunctionIdDimensions(Long functionId,
      String dimensions) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("functionId", functionId);
    filterParams.put("dimensions", dimensions);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_FUNCTION_AND_DIMENSIONS, filterParams, MergedAnomalyResultBean.class);
    List<MergedAnomalyResultDTO> result = new ArrayList<>();
    for (MergedAnomalyResultBean bean : list) {
      result.add(convertMergedAnomalyBean2DTO(bean));
    }
    if (result.size() > 0) {
      return result.get(0);
    }
    return null;
  }


  @Override
  public MergedAnomalyResultDTO findLatestByFunctionIdOnly(Long functionId) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("functionId", functionId);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_FUNCTION_AND_NULL_DIMENSION, filterParams, MergedAnomalyResultBean.class);
    List<MergedAnomalyResultDTO> result = new ArrayList<>();
    for (MergedAnomalyResultBean bean : list) {
      result.add(convertMergedAnomalyBean2DTO(bean));
    }
    if (result.size() > 0) {
      return result.get(0);
    }
    return null;
  }
}
