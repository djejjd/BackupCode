<template>
  <div class="components-container">
    <split-pane split="vertical">

      <template slot="paneL">
        <split-pane split="horizontal">
          <template slot="paneL">
            <el-select v-model="diseaseChartQuery.yearKey" style="width: 90px" class="filter-item" placeholder="年份">
              <el-option v-for="item in yearOptions"  :key="item.key" :label="item.label" :value="item.value"></el-option>
            </el-select>
            <el-select v-model="diseaseChartQuery.ageKey" style="width: 110px" class="filter-item" placeholder="年龄阶段">
              <el-option v-for="item in ageOptions"  :key="item.key" :label="item.label" :value="item.value"></el-option>
            </el-select>
            <el-select v-model="diseaseChartQuery.typeKey" style="width: 110px" class="filter-item" placeholder="展示内容">
              <el-option v-for="item in typeOptions"  :key="item.key" :label="item.label" :value="item.value"></el-option>
            </el-select>
            <el-button type="primary" class="el-icon-pie-chart" @click="showChart">
              展示
            </el-button>
            <div class="top-container" id="top"></div>
          </template>

          <template slot="paneR">
            <div class="bottom-container" id="bottom"></div>
          </template>
        </split-pane>
      </template>

      <template slot="paneR">
        <split-pane split="horizontal">
          <template slot="paneL">
            <div class="top-container" id="right-top"></div>
          </template>

          <template slot="paneR">
            <el-select v-model="diseaseChartQuery.numKey" style="width: 110px" class="filter-item" placeholder="展示数量">
              <el-option v-for="item in numOptions"  :key="item.key" :label="item.label" :value="item.value"></el-option>
            </el-select>
            <el-button type="primary" class="el-icon-pie-chart" @click="showRateChart">
              展示
            </el-button>
            <div class="bottom-container" id="right-bottom"></div>
          </template>
<!--          <div class="left-container" id="right"></div>-->
        </split-pane>
      </template>
    </split-pane>
  </div>
</template>

<script>
import echarts from 'echarts'
import resize from './mixins/resize'
import splitPane from 'vue-splitpane'
import {getDataDiseaseChart} from "@/api/get-chart";
import {getDataGrowthRateChart} from "@/api/get-chart";

export default {
  components: { splitPane },
  mixins: [resize],
  props: {
    className: {
      type: String,
      default: 'chart'
    },
    id: {
      type: String,
      default: 'chart'
    },
    width: {
      type: String,
      default: '200px'
    },
    height: {
      type: String,
      default: '200px'
    }
  },
  data() {
    return {
      leftChart: null,
      topChart: null,
      bottomChart: null,
      listLoading: false,
      diseaseChartQuery: {
        yearKey: "",
        numKey: "",
        ageKey:"",
        typeKey: ''
      },
      yearOptions: [{
        value: '2017',
        label: '2017'
      },{
        value: '2018',
        label: '2018'
      },{
        value: '2019',
        label: '2019'
      }],
      ageOptions: [{
        value: '0',
        label: '0-9岁'
      },{
        value: '1',
        label: '10-19岁'
      },{
        value: '2',
        label: '20-29岁'
      },{
        value: '3',
        label: '30-39岁'
      },{
        value: '4',
        label: '40-49岁'
      },{
        value: '5',
        label: '50-59岁'
      },{
        value: '6',
        label: '60-69岁'
      },{
        value: '7',
        label: '70-79岁'
      },{
        value: '8',
        label: '80-89岁'
      },{
        value: '9',
        label: '90岁以上'
      }],
      numOptions: [{
        value: '5',
        label: '5'
      }, {
        value: '10',
        label: '10'
      },{
        value: '20',
        label: '20'
      }],
      typeOptions: [
        {
          value: "disease",
          label: "患病个数"
        }, {
          value: "diseaseFee",
          label: "总费用"
        }, {
          value: "diseaseFeeRealComp",
          label: "报销费用"
        }, {
          value: "diseaseFeeSelfPay",
          label: "自负费用"
        }
      ]

    }
  },
  mounted() {
    // this.initChart()
  },
  beforeDestroy() {
    if (!this.chart) {
      return
    }
    this.chart.dispose()
    this.chart = null
  },
  methods: {
    showChart() {
      this.initChart()
    },
    showRateChart() {
      getDataGrowthRateChart(this.diseaseChartQuery).then(response => {
        console.log(response.data)
        this.updateRightBottomChart(response.data)
      })
    },
    initChart() {
      this.getLeftData()
    },
    getLeftData() {
      getDataDiseaseChart(this.diseaseChartQuery).then(response => {
        console.log(response.data)
        this.updateLeftTopChart(response.data)
        this.updateLeftBottomChart(response.data)
      })
    },
    updateLeftTopChart(chartData) {
      this.Chart = echarts.init(document.getElementById('top'))
      const colors = ['#5470C6', '#EE6666'];
      this.Chart.setOption({
        color: colors,
        title: {
          text: '建档立卡人群',
          textAlign: 'left'
        },
        tooltip: {
          trigger: 'none',
          axisPointer: {
            type: 'cross'
          }
        },
        legend: {
          data:[chartData['Sex'][0], chartData['Sex'][1]]
        },
        grid: {
          top: 70,
          bottom: 50
        },
        xAxis: [
          {
            type: 'category',
            name: '病名',
            axisTick: {
              alignWithLabel: true
            },
            axisLine: {
              onZero: false,
              lineStyle: {
                color: colors[1]
              }
            },
            axisPointer: {
              label: {
                formatter: function (params) {
                  // console.log(params)
                  return params.value
                    + ": " + params.seriesData[0].value + + " " + String(chartData['type'][1]);
                }
              }
            },
            data: chartData["Disease"][1]
          },
          {
            type: 'category',
            name: '病名',
            axisTick: {
              alignWithLabel: true
            },
            axisLine: {
              onZero: false,
              lineStyle: {
                color: colors[0]
              }
            },
            axisPointer: {
              label: {
                formatter: function (params) {
                  return params.value
                    + ": " + params.seriesData[0].value + " " + String(chartData['type'][1]);
                }
              }
            },
            data: chartData['Disease'][0]
          }
        ],
        yAxis: [
          {
            type: 'value',
            axisLabel: {
              formatter: '{value} '+chartData['type'][1]
            },
            name: chartData['type'][0],
            nameLocation: 'end',
            nameGap: 20,
          }
        ],
        series: [
          {
            name: chartData['Sex'][0],
            type: 'line',
            xAxisIndex: 1,
            smooth: true,
            emphasis: {
              focus: 'series'
            },
            data: chartData['DiseaseName'][0]
          },
          {
            name: chartData['Sex'][1],
            type: 'line',
            smooth: true,
            emphasis: {
              focus: 'series'
            },
            data: chartData['DiseaseName'][1]
          }
        ]
      })
    },
    updateLeftBottomChart(chartData) {
      this.Chart = echarts.init(document.getElementById('bottom'))
      const colors = ['#5470C6', '#EE6666'];
      this.Chart.setOption({
        color: colors,
        title: {
          text: '非建档立卡人群',
          textAlign: 'left'
        },
        tooltip: {
          trigger: 'none',
          axisPointer: {
            type: 'cross'
          }
        },
        legend: {
          data:[chartData['Sex'][0], chartData['Sex'][1]]
        },
        grid: {
          top: 70,
          bottom: 50
        },
        xAxis: [
          {
            type: 'category',
            name: '病名',
            axisTick: {
              alignWithLabel: true
            },
            axisLine: {
              onZero: false,
              lineStyle: {
                color: colors[1]
              }
            },
            axisPointer: {
              label: {
                formatter: function (params) {
                  // console.log(params)
                  return params.value
                    + ": " + params.seriesData[0].value + " " + String(chartData['type'][1]);
                }
              }
            },
            data: chartData["Disease"][3]
          },
          {
            type: 'category',
            name: '病名',
            axisTick: {
              alignWithLabel: true
            },
            axisLine: {
              onZero: false,
              lineStyle: {
                color: colors[0]
              }
            },
            axisPointer: {
              label: {
                formatter: function (params) {
                  return params.value
                    + ": " + params.seriesData[0].value + " " + String(chartData['type'][1]);
                }
              }
            },
            data: chartData['Disease'][2]
          }
        ],
        yAxis: [
          {
            type: 'value',
            axisLabel: {
              formatter: '{value} ' + chartData['type'][1]
            },
            name: chartData['type'][0],
            nameLocation: 'end',
            nameGap: 20,
          }
        ],
        series: [
          {
            name: chartData['Sex'][2],
            type: 'line',
            xAxisIndex: 1,
            smooth: true,
            emphasis: {
              focus: 'series'
            },
            data: chartData['DiseaseName'][2]
          },
          {
            name: chartData['Sex'][3],
            type: 'line',
            smooth: true,
            emphasis: {
              focus: 'series'
            },
            data: chartData['DiseaseName'][3]
          }
        ]
      })
    },
    updateRightBottomChart(chartData) {
      this.Chart = echarts.init(document.getElementById("right-bottom"))
      const colors = ['#5470C6', '#EE6666'];
      this.Chart.setOption({
        color: colors,
        title: {
          text: '患病平均增长率',
          textAlign: 'left'
        },
        tooltip: {
          trigger: 'none',
          axisPointer: {
            type: 'cross'
          }
        },
        legend: {
          data:["建档立卡", "非建档立卡"]
        },
        grid: {
          top: 70,
          bottom: 50
        },
        xAxis: [
          {
            name: '病名',
            type: 'category',
            axisTick: {
              alignWithLabel: true
            },
            axisLine: {
              onZero: false,
              lineStyle: {
                color: colors[1]
              }
            },
            axisPointer: {
              label: {
                formatter: function (params) {
                  // console.log(params)
                  return params.value
                    + ": " + params.seriesData[0].value.toFixed(2) + "%";
                }
              }
            },
            data: chartData["DiseaseName"][0]
          },
          {
            name: '病名',
            type: 'category',
            axisTick: {
              alignWithLabel: true
            },
            axisLine: {
              onZero: false,
              lineStyle: {
                color: colors[0]
              }
            },
            axisPointer: {
              label: {
                formatter: function (params) {
                  return params.value
                    + ": " + params.seriesData[0].value.toFixed(2) + "%";
                }
              }
            },
            data: chartData['DiseaseName'][1]
          }
        ],
        yAxis: [
          {
            name: "增长率",
            nameLocation: 'middle',
            nameGap: 30,
            type: 'value',
            axisLabel: {
              formatter: '{value}%'
            },
            axisPointer: {
              snap: true
            }
          }
        ],
        series: [
          {
            name: "非建档立卡",
            type: 'bar',
            xAxisIndex: 1,
            smooth: true,
            emphasis: {
              focus: 'series'
            },
            data: chartData['averageRate'][1]
          },
          {
            name: "建档立卡",
            type: 'bar',
            smooth: true,
            emphasis: {
              focus: 'series'
            },
            data: chartData['averageRate'][0]
          }
        ]
      })
    }

  }
}
</script>

<style  scoped>
.components-container {
  position: relative;
  height: 100vh;
  padding-bottom: 130px;
}

.left-container {
  height: 100%;
}

.top-container {
  width: 100%;
  height: 100%;
}

.bottom-container {
  width: 100%;
  height: 100%;
}
</style>
