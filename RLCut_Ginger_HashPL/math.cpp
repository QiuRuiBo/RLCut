#include <iostream>
#include <stdlib.h>
#include <stack>
#include "libgraph.h"
#include "lib.h"
using namespace std;
#pragma warning(disable : 4996)
int random_at_most(int max)
{
	unsigned int num_bins = (unsigned long)max + 1;
	unsigned int num_rand = (unsigned long)RAND_MAX + 1;
	unsigned int bin_size = num_rand / num_bins;
	unsigned int defect = num_rand % num_bins;

	int x;
	do
	{
		x = random();
	}
	// This is carefully written not to overflow
	while (num_rand - defect <= (unsigned int)x);

	// Truncated division is intentional
	return (x / bin_size);
}
int max_value_index(double *array, int num)
{
	double temp = array[0];
	int index = 0;
	for (int i = 1; i < num; i++)
	{
		if (temp < array[i])
		{
			temp = array[i];
			index = i;
		}
	}
	return index;
}
int my_max_value_index(double *array, int num)
{
	stack<pair<double, int>> s;
	s.push(make_pair(array[0], 0));
	for (int i = 1; i < num; i++)
	{
		if (array[i] == s.top().first)
		{
			s.push(make_pair(array[i], i));
		}
		else if (array[i] > s.top().first)
		{
			while (!s.empty())
				s.pop();
			s.push(make_pair(array[i], i));
		}
	}
	if (s.size() != 1)
	{
		int t = rand() % s.size();
		while (t--)
			s.pop();
	}
	return s.top().second;
}
int max_value_index_besides(double *array, int num, int besides)
{
	double temp = besides == 0 ? array[1] : array[0];
	int index = besides == 0 ? 1 : 0;
	for (int i = 1; i < num; i++)
	{
		if (i != besides && temp < array[i])
		{
			temp = array[i];
			index = i;
		}
	}
	return index;
}
int max_random_value_index(double *array, int num)
{
	double temp = array[0];
	int index = 0;
	for (int i = 1; i < num; i++)
	{
		if (temp < array[i])
		{
			temp = array[i];
			index = i;
		}
	}
	vector<int> candidate = vector<int>();
	for (int i = 0; i < num; i++)
	{
		if (array[i] == array[index])
			candidate.push_back(i);
	}
	int res = random_at_most(candidate.size() - 1);
	return candidate[res];
}
int min_value_index(int *array, int num)
{
	int temp = array[0];
	int index = 0;
	for (int i = 1; i < num; i++)
	{
		if (temp > array[i])
		{
			temp = array[i];
			index = i;
		}
	}
	return index;
}
int min_value_index_double(double *array, int num)
{
	double temp = array[0];
	int index = 0;
	for (int i = 1; i < num; i++)
	{
		if (temp > array[i])
		{
			temp = array[i];
			index = i;
		}
	}
	return index;
}
int min_value_index_double_besides(double *array, int num, int besides)
{
	double temp = besides == 0 ? array[1] : array[0];
	int index = besides == 0 ? 1 : 0;
	for (int i = 1; i < num; i++)
	{
		if (i != besides && temp > array[i])
		{
			temp = array[i];
			index = i;
		}
	}
	return index;
}
double min_value(double *array)
{
	double temp = array[0];
	int index = 0;
	for (int i = 1; i < algorithm->DCnums; i++)
	{
		if (temp > array[i])
		{
			temp = array[i];
			index = i;
		}
	}
	return array[index];
}
double sumWeights(double *array, double *upprice)
{
	double s = 0;
	for (int i = 0; i < algorithm->DCnums; i++)
	{
		s += array[i] * upprice[i];
	}
	return s;
}
double max_value(double *array)
{
	double temp = array[0];
	for (int i = 1; i < algorithm->DCnums; i++)
		if (array[i] > temp)
		{
			temp = array[i];
		}
	return temp;
}
double abs_double(double number)
{
	return number >= 0 ? number : -number;
}
double sum_value(double *array, int num)
{
	double sum = 0;
	for (int i = 0; i < num; i++)
		sum += array[i];
	return sum;
}
void bubble_sort(double *value, int *indices, int count)
{
	int k = 0;
	int l = 0;
	double tmp_value = 0;
	int tmp_index = 0;
	for (k = 0; k < count; k++)
		for (l = k + 1; l < count; l++)
		{
			if (value[k] > value[l])
			{
				tmp_value = value[k];
				;
				value[k] = value[l];
				value[l] = tmp_value;
				tmp_index = indices[k];
				indices[k] = indices[l];
				indices[l] = tmp_index;
			}
		}
}
void bubble_sort(double *array, int num)
{
	double temp = 0;
	for (int i = 0; i < num - 1; i++)
		for (int j = i + 1; j < num; j++)
			if (array[i] > array[j])
			{
				temp = array[i];
				array[i] = array[j];
				array[j] = temp;
			}
}
